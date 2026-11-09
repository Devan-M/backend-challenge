# Distributed Wagering Processor

Serviço NestJS para processar apostas e liquidações de múltiplos provedores. O projeto demonstra processamento financeiro exato, idempotência persistente, concorrência por wallet, ledger imutável, integração assíncrona via SQS e recuperação por inbox/outbox.

## Conteúdo

- [Visão geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Executar localmente](#executar-localmente)
- [API HTTP](#api-http)
- [Regras financeiras](#regras-financeiras)
- [Filas e eventos](#filas-e-eventos)
- [Observabilidade](#observabilidade)
- [Testes](#testes)
- [Documentação relacionada](#documentação-relacionada)
- [Limitações](#limitações)

## Visão geral

O serviço recebe operações de jogo associadas a uma rodada:

```text
BET -> WIN | LOSS | REFUND | ROLLBACK
```

A entrega pode ser duplicada ou chegar fora de ordem. A aplicação persiste cada operação e garante que uma repetição não reaplique o efeito financeiro. PostgreSQL é a fonte de verdade; SQS FIFO organiza mensagens, mas não substitui as garantias do banco.

Dinheiro é representado por `bigint` em unidades menores no domínio e por `NUMERIC(38, 2)` no PostgreSQL. Os contratos HTTP usam valores decimais em strings, nunca `number`:

```json
{ "amount": "25.00", "currency": "BRL" }
```

## Arquitetura

- `src/domain`: `Money`, `Wallet`, transações, ledger e envelopes de eventos, sem dependência de NestJS ou ORM.
- `src/application`: caso de uso financeiro, persistência transacional, idempotência, inbox/outbox e workers.
- `src/http`: controllers, health checks, métricas e mapeamento de erros HTTP.
- `src/migrations`: migrations PostgreSQL reversíveis e constraints financeiras.
- `test/integration`: cenários com PostgreSQL e LocalStack reais em containers.

Uma submissão financeira roda em uma transação SQL. A aplicação bloqueia a linha da wallet com `SELECT ... FOR UPDATE`, valida saldo e referência, grava transaction/ledger/outbox e, para SQS, marca o inbox antes do commit. A unidade de concorrência é uma wallet; wallets diferentes continuam independentes.

A outbox é publicada em lotes usando `FOR UPDATE SKIP LOCKED` e leases. A entrega é at-least-once: consumidores de eventos devem deduplicar pelo `eventId`. Para detalhes de decisões, erros e trade-offs, consulte [ARCHITECTURE.md](ARCHITECTURE.md).

## Executar localmente

### Requisitos

- Bun 1.4.4 ou compatível com o lockfile do projeto.
- Docker Desktop com Docker Compose.

### Iniciar os serviços

Na raiz do repositório, em PowerShell:

```powershell
bun install
docker compose up --build -d
```

O Compose inicia PostgreSQL, LocalStack, cria as filas SQS e inicia a API. Na primeira inicialização, o container da API aplica as migrations automaticamente.

URLs locais:

| Serviço | URL |
|---|---|
| API | `http://localhost:3000` |
| Liveness | `http://localhost:3000/health/live` |
| Readiness | `http://localhost:3000/health/ready` |
| Métricas Prometheus | `http://localhost:3000/metrics` |
| SQS/LocalStack | `http://localhost:4566` |
| PostgreSQL | `localhost:5432` |

Para ver os logs e o estado dos containers:

```powershell
docker compose ps
docker compose logs -f app
```

Para parar os serviços sem apagar o banco local:

```powershell
docker compose down
```

Os dados ficam em volumes nomeados. `docker compose down -v` também remove esses volumes e apaga os dados locais.

### Variáveis de ambiente

O arquivo [.env.example](.env.example) é um template versionado, e `.env` está no `.gitignore` para não publicar configurações locais. Ao executar Bun diretamente no host, use o template como referência e defina `DATABASE_*`, `SQS_ENDPOINT`, `WAGER_QUEUE_URL`, `WAGER_DLQ_URL` e `EVENTS_QUEUE_URL` no ambiente do processo.

No Docker Compose atual, `DATABASE_USER`, `DATABASE_PASSWORD`, `POSTGRES_USER` e `POSTGRES_PASSWORD` estão definidos diretamente no `docker-compose.yml` com valores **somente para desenvolvimento local** (`wagering`). Criar um `.env` não altera essas credenciais, pois elas ainda não são interpoladas pelo Compose. Não use esses valores em produção; configure segredos por um mecanismo próprio do ambiente de deploy e não os versione.

As variáveis `WAGER_MESSAGE_VISIBILITY_TIMEOUT_SECONDS` e `SQS_BEFORE_ACK_DELAY_MS` permitem configurar o timeout de invisibilidade e uma pausa antes do ack. A pausa existe para testes do cenário de crash e deve permanecer em `0` fora de testes.

### Migrations

O container aplica migrations pendentes no startup. Também é possível aplicar ou reverter a migration mais recente manualmente, com o banco acessível e as variáveis de conexão configuradas:

```powershell
bun run db:migrate
bun run db:rollback
```

## API HTTP

Os exemplos abaixo usam `curl.exe`, disponível no Windows. Substitua os UUIDs de exemplo pelos valores retornados pelas suas chamadas.

### Criar wallet

```powershell
curl.exe -X POST http://localhost:3000/wallets `
  -H "Content-Type: application/json" `
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"100.00","currency":"BRL"}}'
```

Resposta `201 Created`:

```json
{
  "id": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "balance": { "amount": "100.00", "currency": "BRL" },
  "version": 1
}
```

Saldo inicial positivo também grava uma transação interna `OPENING`, um crédito no ledger e um evento `WalletBalanceChanged`, tudo no mesmo commit. Uma wallet existente para o mesmo player e moeda retorna `409 WALLET_ALREADY_EXISTS`.

### Consultar wallet e ledger

```http
GET /wallets/:walletId
GET /wallets/:walletId/ledger?cursor=<cursor-opaco>&limit=50
```

Exemplo de resposta do ledger:

```json
{
  "entries": [
    {
      "id": "ledger-entry-id",
      "transactionId": "transaction-id",
      "direction": "DEBIT",
      "money": { "amount": "25.00", "currency": "BRL" },
      "balanceBefore": { "amount": "100.00", "currency": "BRL" },
      "balanceAfter": { "amount": "75.00", "currency": "BRL" },
      "createdAt": "2026-11-07T12:00:00.000Z"
    }
  ],
  "nextCursor": "eyJjcmVhdGVkQXQiOi..."
}
```

O cursor ordena por timestamp e ID e é opaco para o cliente. `limit` é limitado a 100.

### Submeter aposta

O header `Idempotency-Key` é obrigatório. A chave recomendada é `providerId:externalTransactionId`.

```powershell
curl.exe -X POST http://localhost:3000/wagering/transactions `
  -H "Content-Type: application/json" `
  -H "Idempotency-Key: provider-a:transaction-123" `
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

Resposta processada (`200 OK`):

```json
{
  "transactionId": "0192f298-345e-7e38-af88-e43f851a819d",
  "status": "PROCESSED",
  "balance": { "amount": "75.00", "currency": "BRL" },
  "idempotentReplay": false
}
```

Repetir a mesma chave e payload retorna o resultado persistido, incluindo o saldo observado na operação original, com `idempotentReplay: true`. Reutilizar a chave com outro payload retorna `409 IDEMPOTENCY_KEY_REUSED`.

Exemplo de aposta rejeitada por saldo insuficiente (`422 Unprocessable Entity`):

```json
{
  "transactionId": "transaction-id",
  "status": "REJECTED",
  "balance": { "amount": "10.00", "currency": "BRL" },
  "idempotentReplay": false,
  "failureCode": "INSUFFICIENT_FUNDS"
}
```

### Consultar transação e reconciliar

```http
GET /wagering/transactions/:transactionId
GET /providers/:providerId/wagering/transactions/:externalTransactionId
POST /wallets/:walletId/reconciliation
```

A reconciliação compara o saldo materializado com a soma assinada do ledger. Ela reporta a diferença, grava um warning estruturado e incrementa `wallet_reconciliation_divergences_total`; não corrige dados automaticamente.

Exemplo de resposta consistente:

```json
{
  "walletId": "wallet-id",
  "storedBalance": { "amount": "75.00", "currency": "BRL" },
  "calculatedBalance": { "amount": "75.00", "currency": "BRL" },
  "difference": { "amount": "0.00", "currency": "BRL" },
  "consistent": true,
  "checkedEntries": 2
}
```

### Status HTTP

| Status | Significado |
|---|---|
| `200` | Operação processada ou replay de idempotência |
| `201` | Wallet criada |
| `202` | A operação aguarda a transação referenciada |
| `400` | Payload, valor monetário ou cursor inválido |
| `409` | Chave idempotente conflitante ou wallet duplicada |
| `422` | Rejeição de regra de negócio, como saldo insuficiente |
| `500` | Falha permanente persistida como `FAILED` |
| `503` | Falha transitória de infraestrutura; a submissão pode ser repetida |

## Regras financeiras

| Operação | Efeito | Regras principais |
|---|---|---|
| `BET` | Débito | Rejeitada como `INSUFFICIENT_FUNDS` se o saldo não cobrir a aposta |
| `WIN` | Crédito | Pode referenciar uma aposta processada da mesma rodada |
| `LOSS` | Nenhum | Resultado registrado sem mudar wallet ou criar ledger entry |
| `REFUND` | Crédito | Exige referência a `BET` processada; só pode ocorrer uma vez |
| `ROLLBACK` | Inverso da referência | Reverte `BET`, `WIN` ou `REFUND` processado, uma vez por tipo |

Reversões exigem o mesmo provider, player, wallet, moeda e rodada da referência. `REFUND` e `ROLLBACK` precisam ter valor igual ao valor revertido. Se um débito de reversão deixaria o saldo negativo, a operação é rejeitada com `REVERSAL_WOULD_OVERDRAW`.

`REFUND` ou `ROLLBACK` recebido antes da referência fica em `PENDING_REFERENCE`. O worker retenta com backoff exponencial (início em 5 segundos, limite de 60 segundos) e rejeita após 12 tentativas com `REFERENCE_NOT_FOUND`.

## Filas e eventos

O Compose cria estas filas FIFO no LocalStack:

- `wager-transactions.fifo`: solicitações de transação.
- `wager-transactions-dlq.fifo`: mensagens inválidas ou falhas permanentes; a fila principal também tem redrive após cinco recebimentos.
- `wagering-events.fifo`: eventos publicados pela outbox.

Exemplo de mensagem de entrada:

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-11-07T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

O consumidor usa o mesmo caso de uso da API, registra inbox por `(consumerName, messageId)` e dá ack só depois do commit. No shutdown, o long-poll é cancelado, mensagens ainda não iniciadas voltam a ficar visíveis e o trabalho em andamento é aguardado.

Eventos confirmados são publicados pela outbox depois do commit. Consumidores devem deduplicar por `eventId`, pois uma publicação pode ser repetida se o processo cair depois do envio ao SQS e antes de marcar `published_at`.

## Observabilidade

`GET /health/live` indica que o processo está ativo. `GET /health/ready` só retorna pronto quando PostgreSQL e as filas SQS estão acessíveis. Ambos os endpoints de health não exigem autenticação.

`GET /metrics` expõe métricas Prometheus, incluindo transações por status, duplicatas, retries, DLQ atual, conflitos de lock, divergências de reconciliação, latência de processamento e atraso da outbox. Os logs são JSON e usam identificadores de correlação/transação sem registrar valores financeiros completos.

## Testes

```powershell
bun run build
bun run test
```

`bun run build` aplica TypeScript estrito à aplicação e aos testes de integração; `bun run test` roda os testes unitários de domínio.

Os testes de integração usam PostgreSQL e LocalStack reais em containers. Para habilitar cenários distribuídos, restart e falha de broker/banco, inicie três instâncias e configure as URLs adicionais:

```powershell
docker compose --profile multi up --build -d
$env:API_SECONDARY_URL = "http://localhost:3001"
$env:API_TERTIARY_URL = "http://localhost:3002"
bun run test:integration
```

A suíte cobre contenção financeira, 50 primeiras requisições simultâneas, wallets independentes, constraints do schema, atomicidade, reconciliação, reversões, inbox/DLQ, retry transitório, publishers paralelos, indisponibilidade de SQS, restart e crash entre commit e ack.

## Documentação relacionada

- [ARCHITECTURE.md](ARCHITECTURE.md): decisões técnicas, garantias, códigos de falha e limitações.
- [CODE_GUIDE.md](CODE_GUIDE.md): descrição dos arquivos de configuração e da estrutura do projeto.

## Limitações

A configuração de autenticação é intencionalmente um `AuthGuard` no-op; produção deve substituí-lo por OIDC com um Identity Provider externo. O domínio assume moedas com duas casas decimais. Os testes cobrem retry e DLQ permanente, mas não esgotam a redrive policy após uma indisponibilidade prolongada. Não há teste de carga incluído.
