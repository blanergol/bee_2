# purged

HTTP-сервис на Go 1.26.1 для чанкового удаления старых строк по
timestamp-колонке из крупных PostgreSQL-таблиц без длительных блокировок.
Поддерживает параллельные запросы (по одному прогону на таблицу через
PostgreSQL advisory-lock) и пишет структурированные логи `log/slog`.

> Сервис проектировался под таблицы 10⁷+ строк. Реализация не делает
> предположений о размере: время прогона определяется
> `chunk_size`, паузой между чанками и `lock_timeout`. Бенчмарка на "живых"
> 10⁷ строках в репозитории нет - есть unit-бенчмарк движка на фейковом
> store и опциональный integration-тест на реальной БД с десятком тысяч строк.

## Сборка и запуск

POSIX:

```sh
go build -o bin/purged ./cmd/purged
PURGE_DSN='postgres://user:pass@host:5432/db?sslmode=disable' \
PURGE_TARGETS='events:public.events:created_at,logs:audit.app_logs:ts' \
PURGE_LISTEN=':8080' \
./bin/purged
```

PowerShell (Windows):

```powershell
go build -o bin\purged.exe .\cmd\purged
$env:PURGE_DSN     = 'postgres://user:pass@host:5432/db?sslmode=disable'
$env:PURGE_TARGETS = 'events:public.events:created_at,logs:audit.app_logs:ts'
$env:PURGE_LISTEN  = ':8080'
.\bin\purged.exe
```

## Конфигурация

Источники: значение по умолчанию → переменная окружения → CLI-флаг (последний имеет приоритет).

| Переменная | Флаг | По умолчанию | Назначение |
|---|---|---|---|
| `PURGE_DSN` | `-dsn` | - | DSN PostgreSQL (libpq URL или key=value) |
| `PURGE_LISTEN` | `-listen` | `:8080` | Адрес HTTP-сервера |
| `PURGE_TARGETS` | `-targets` | - | Whitelist `alias:schema.table:ts_col,...` |
| `PURGE_CHUNK_SIZE` | `-chunk-size` | `10000` | Размер пакета удаления по умолчанию |
| `PURGE_MAX_CHUNK_SIZE` | `-max-chunk-size` | `200000` | Верхняя граница chunk_size в запросе |
| `PURGE_PAUSE_MS` | `-pause-ms` | `50` | Пауза между пакетами (мс) |
| `PURGE_CHUNK_TIMEOUT` | `-chunk-timeout` | `10s` | `statement_timeout` одной чанковой транзакции |
| `PURGE_SHUTDOWN_TIMEOUT` | `-shutdown-timeout` | `30s` | Бюджет graceful-shutdown |
| `PURGE_RETAIN_COMPLETED` | `-retain-completed` | `24h` | Время хранения завершённых прогонов в реестре; `0` - не убирать |
| `PURGE_LOG_LEVEL` | - | `info` | `debug`/`info`/`warn`/`error` |

## HTTP API

### `POST /v1/purges` - запустить прогон

```sh
curl -sS -X POST http://localhost:8080/v1/purges \
  -H 'Content-Type: application/json' \
  -d '{
    "target": "events",
    "before": "2026-01-01T00:00:00Z",
    "chunk_size": 10000,
    "pause_ms": 50,
    "max_chunks": 0
  }'
```

Ответы:

* `202 Accepted` - `{"purge_id":"<id>","status":"running"}` - прогон стартует фоновой goroutine, которая продолжает работу после ответа.
* `400 Bad Request` - невалидные параметры (включая попытки SQL-injection - alias обязан быть в whitelist'е).
* `409 Conflict` - для этой таблицы уже идёт прогон (advisory-lock занят).

### `GET /v1/purges/{id}` - статус прогона

```sh
curl -sS http://localhost:8080/v1/purges/<id>
```

```json
{
  "purge_id": "...",
  "target": "events",
  "status": "running|done|failed|aborted",
  "deleted_total": 12345,
  "chunks_done": 2,
  "started_at": "2026-05-08T12:00:00Z",
  "finished_at": "2026-05-08T12:00:30Z"
}
```

### `GET /healthz`, `GET /readyz`

Liveness и readiness; readyz делает `pgxpool.Ping` с таймаутом 2 с и возвращает `503` при недоступной БД.

## Поведение при недоступной БД

Сервис выбирает **fail-open** на старте: при ошибке стартового `Ping` сервис продолжает запуск (логирует `startup.ping.failed`), потому что `pgxpool` коннектится лениво и сетевые сбои часто временные. Состояние БД далее наблюдается через `GET /readyz`. Если требуется fail-closed - добавьте проверку через внешнего супервайзера (k8s readinessProbe + initialDelaySeconds=0).

## Хранение состояния прогонов

Состояние in-memory: `map[purge_id]Run`. Завершённые прогоны очищаются периодическим sweeper'ом по `PURGE_RETAIN_COMPLETED`. **При рестарте процесса вся история теряется**, активные прогоны при graceful-shutdown переводятся в `aborted` и advisory-lock'и освобождаются. Внешним процессам, ожидающим завершения по `purge_id`, при перезапуске сервиса нужно перезапустить прогон. Для долговременной истории добавьте отдельный Postgres-стор для статусов - это вне scope первой версии.

## Требования к БД

* PostgreSQL 14+.
* Роль сервиса должна иметь `SELECT, DELETE` на целевые таблицы. Функции `pg_try_advisory_lock` / `pg_advisory_unlock` доступны всем по умолчанию.
* На `ts_col` каждой целевой таблицы **должен быть индекс** - без него план чанкового удаления вырождается в seqscan.
* Используется session-level advisory-lock с ключом `hashtext(alias)::bigint`. На время прогона из пула удерживается один коннект.

## Известные ограничения

* Нет API отмены прогона (только остановка процесса). Сознательный non-goal первой версии.
* Hash-коллизии advisory-lock'а теоретически возможны при широком whitelist'е; для десятков таблиц пренебрежимы.
* Прогресс пишется в `INFO`-логах (`purge.chunk` после каждого пакета), что заметно нагружает stdout при коротких чанках. При необходимости понизьте до `warn` через `PURGE_LOG_LEVEL`.

## Разработка

Сборка, форматирование и тесты — стандартные команды Go (одинаково в bash и PowerShell):

```sh
gofmt -l .
go vet ./...
go build ./...
go test ./... -count=1
go test ./... -race -count=1
```

`gofmt -l .` должен возвращать пустой вывод; ненулевой — список файлов, требующих переформатирования (`gofmt -w <file>`).

Integration-тесты на реальном PostgreSQL под build-tag `integration` (требуют `PURGE_TEST_DSN` и право на `CREATE TABLE` в `public`):

POSIX:

```sh
PURGE_TEST_DSN='postgres://...' go test -tags=integration ./internal/store
```

PowerShell:

```powershell
$env:PURGE_TEST_DSN = 'postgres://...'
go test -tags=integration ./internal/store
```
