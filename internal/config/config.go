// Package config описывает конфигурацию сервиса purged: источники
// (флаги командной строки, переменные окружения), парсинг whitelist'а
// целевых таблиц и валидацию итоговых значений.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// Дефолты вынесены в константы, чтобы не было magic-чисел в Load.
const (
	DefaultListen          = ":8080"
	DefaultChunkSize       = 10000
	DefaultMaxChunkSize    = 200000
	DefaultPause           = 50 * time.Millisecond
	DefaultChunkTimeout    = 10 * time.Second
	DefaultShutdownTimeout = 30 * time.Second
	DefaultRetainCompleted = 24 * time.Hour
)

// Sentinel-ошибки конфигурации.
var (
	// ErrEmptyDSN сигнализирует об отсутствии DSN.
	ErrEmptyDSN = errors.New("config: dsn is empty")
	// ErrEmptyTargets сигнализирует, что whitelist целевых таблиц пуст.
	ErrEmptyTargets = errors.New("config: targets whitelist is empty")
	// ErrInvalidTargets сообщает о синтаксической ошибке в строке whitelist'а.
	ErrInvalidTargets = errors.New("config: invalid targets specification")
	// ErrDuplicateAlias сообщает о повторяющемся alias в whitelist'е.
	ErrDuplicateAlias = errors.New("config: duplicate target alias")
	// ErrNonPositive сообщает о нулевом/отрицательном значении параметра, ожидающего > 0.
	ErrNonPositive = errors.New("config: value must be positive")
	// ErrChunkTooBig сообщает, что ChunkSize превышает MaxChunkSize.
	ErrChunkTooBig = errors.New("config: chunk size exceeds max chunk size")
)

// Target описывает запись whitelist'а: alias → конкретная таблица и временная колонка.
type Target struct {
	// Alias — короткое имя, по которому клиент API ссылается на таблицу.
	Alias string
	// Schema — имя схемы PostgreSQL.
	Schema string
	// Table — имя таблицы PostgreSQL.
	Table string
	// TSColumn — имя колонки типа timestamp/timestamptz, по которой удаляются старые строки.
	TSColumn string
}

// Config агрегирует все настройки сервиса.
type Config struct {
	// DSN — строка подключения PostgreSQL (libpq URL или key=value).
	DSN string
	// Listen — адрес HTTP-сервера в формате "host:port".
	Listen string
	// ChunkSize — размер пакета удаления по умолчанию.
	ChunkSize int
	// MaxChunkSize — верхняя граница размера пакета, разрешённая клиенту в запросе.
	MaxChunkSize int
	// Pause — пауза между пакетами по умолчанию.
	Pause time.Duration
	// ChunkTimeout — таймаут одной чанковой транзакции (statement_timeout).
	ChunkTimeout time.Duration
	// ShutdownTimeout — общий бюджет graceful-shutdown.
	ShutdownTimeout time.Duration
	// RetainCompleted — сколько хранить завершённые прогоны в реестре до их
	// уборки фоновым sweeper'ом. 0 — никогда не убирать.
	RetainCompleted time.Duration
	// Targets — whitelist alias → Target.
	Targets map[string]Target
}

// Load читает конфигурацию из переменных окружения и аргументов командной строки.
// Приоритет: значение по умолчанию -> переменная окружения -> явный CLI-флаг.
// Параметр args — argv без имени программы (как os.Args[1:]).
// Параметр lookupEnv позволяет тестам подменить os.LookupEnv.
// Параметр errOut получает текст ошибок парсинга флагов.
func Load(args []string, lookupEnv func(string) (string, bool), errOut io.Writer) (Config, error) {
	if lookupEnv == nil {
		lookupEnv = os.LookupEnv
	}
	if errOut == nil {
		errOut = io.Discard
	}

	cfg := Config{
		Listen:          DefaultListen,
		ChunkSize:       DefaultChunkSize,
		MaxChunkSize:    DefaultMaxChunkSize,
		Pause:           DefaultPause,
		ChunkTimeout:    DefaultChunkTimeout,
		ShutdownTimeout: DefaultShutdownTimeout,
		RetainCompleted: DefaultRetainCompleted,
	}

	applyEnv(&cfg, lookupEnv)

	fs := flag.NewFlagSet("purged", flag.ContinueOnError)
	fs.SetOutput(errOut)
	dsn := fs.String("dsn", cfg.DSN, "PostgreSQL DSN (env PURGE_DSN)")
	listen := fs.String("listen", cfg.Listen, "HTTP listen address (env PURGE_LISTEN)")
	chunk := fs.Int("chunk-size", cfg.ChunkSize, "default chunk size (env PURGE_CHUNK_SIZE)")
	maxChunk := fs.Int("max-chunk-size", cfg.MaxChunkSize, "maximum allowed chunk size (env PURGE_MAX_CHUNK_SIZE)")
	pauseMs := fs.Int("pause-ms", int(cfg.Pause/time.Millisecond), "pause between chunks in ms (env PURGE_PAUSE_MS)")
	chunkTO := fs.Duration("chunk-timeout", cfg.ChunkTimeout, "chunk transaction timeout (env PURGE_CHUNK_TIMEOUT)")
	shutTO := fs.Duration("shutdown-timeout", cfg.ShutdownTimeout, "graceful shutdown timeout (env PURGE_SHUTDOWN_TIMEOUT)")
	retain := fs.Duration("retain-completed", cfg.RetainCompleted, "how long to retain finished purges in registry; 0 disables sweep (env PURGE_RETAIN_COMPLETED)")
	targets := fs.String("targets", targetsToString(cfg.Targets), "whitelist: alias:schema.table:ts_col,... (env PURGE_TARGETS)")

	if err := fs.Parse(args); err != nil {
		return Config{}, fmt.Errorf("parse flags: %w", err)
	}

	cfg.DSN = *dsn
	cfg.Listen = *listen
	cfg.ChunkSize = *chunk
	cfg.MaxChunkSize = *maxChunk
	cfg.Pause = time.Duration(*pauseMs) * time.Millisecond
	cfg.ChunkTimeout = *chunkTO
	cfg.ShutdownTimeout = *shutTO
	cfg.RetainCompleted = *retain

	parsed, err := ParseTargets(*targets)
	if err != nil {
		return Config{}, err
	}
	cfg.Targets = parsed

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func applyEnv(cfg *Config, lookup func(string) (string, bool)) {
	if v, ok := lookup("PURGE_DSN"); ok {
		cfg.DSN = v
	}
	if v, ok := lookup("PURGE_LISTEN"); ok {
		cfg.Listen = v
	}
	if v, ok := lookup("PURGE_CHUNK_SIZE"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ChunkSize = n
		}
	}
	if v, ok := lookup("PURGE_MAX_CHUNK_SIZE"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxChunkSize = n
		}
	}
	if v, ok := lookup("PURGE_PAUSE_MS"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Pause = time.Duration(n) * time.Millisecond
		}
	}
	if v, ok := lookup("PURGE_CHUNK_TIMEOUT"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.ChunkTimeout = d
		}
	}
	if v, ok := lookup("PURGE_SHUTDOWN_TIMEOUT"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.ShutdownTimeout = d
		}
	}
	if v, ok := lookup("PURGE_RETAIN_COMPLETED"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.RetainCompleted = d
		}
	}
	if v, ok := lookup("PURGE_TARGETS"); ok {
		if parsed, err := ParseTargets(v); err == nil {
			cfg.Targets = parsed
		}
	}
}

// ParseTargets разбирает строку whitelist'а формата
// "alias:schema.table:ts_col,alias:schema.table:ts_col".
// Пустая строка возвращает пустую map (без ошибки), валидацию пустоты
// выполняет Config.Validate.
func ParseTargets(s string) (map[string]Target, error) {
	out := make(map[string]Target)
	s = strings.TrimSpace(s)
	if s == "" {
		return out, nil
	}
	for raw := range strings.SplitSeq(s, ",") {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("%w: %q", ErrInvalidTargets, entry)
		}
		alias := strings.TrimSpace(parts[0])
		ref := strings.TrimSpace(parts[1])
		ts := strings.TrimSpace(parts[2])
		if alias == "" || ref == "" || ts == "" {
			return nil, fmt.Errorf("%w: empty component in %q", ErrInvalidTargets, entry)
		}
		schema, table, ok := splitSchemaTable(ref)
		if !ok {
			return nil, fmt.Errorf("%w: bad schema.table in %q", ErrInvalidTargets, entry)
		}
		if !isIdent(alias) || !isIdent(schema) || !isIdent(table) || !isIdent(ts) {
			return nil, fmt.Errorf("%w: non-identifier component in %q", ErrInvalidTargets, entry)
		}
		if _, dup := out[alias]; dup {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateAlias, alias)
		}
		out[alias] = Target{Alias: alias, Schema: schema, Table: table, TSColumn: ts}
	}
	return out, nil
}

func splitSchemaTable(ref string) (schema, table string, ok bool) {
	idx := strings.IndexByte(ref, '.')
	if idx <= 0 || idx == len(ref)-1 {
		return "", "", false
	}
	return ref[:idx], ref[idx+1:], true
}

// isIdent проверяет, что строка — допустимый «безопасный» SQL-идентификатор:
// начинается с буквы или _, дальше [A-Za-z0-9_], длина до 63 символов
// (ограничение PostgreSQL по умолчанию). Этого достаточно как первого
// барьера; финальное экранирование делает pgx.Identifier.Sanitize.
func isIdent(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

func targetsToString(t map[string]Target) string {
	if len(t) == 0 {
		return ""
	}
	parts := make([]string, 0, len(t))
	for _, v := range t {
		parts = append(parts, fmt.Sprintf("%s:%s.%s:%s", v.Alias, v.Schema, v.Table, v.TSColumn))
	}
	return strings.Join(parts, ",")
}

// Validate проверяет корректность собранной конфигурации.
func (c Config) Validate() error {
	if c.DSN == "" {
		return ErrEmptyDSN
	}
	if c.Listen == "" {
		return fmt.Errorf("%w: listen", ErrNonPositive)
	}
	if c.ChunkSize <= 0 {
		return fmt.Errorf("%w: chunk-size", ErrNonPositive)
	}
	if c.MaxChunkSize <= 0 {
		return fmt.Errorf("%w: max-chunk-size", ErrNonPositive)
	}
	if c.ChunkSize > c.MaxChunkSize {
		return ErrChunkTooBig
	}
	if c.Pause < 0 {
		return fmt.Errorf("%w: pause", ErrNonPositive)
	}
	if c.ChunkTimeout <= 0 {
		return fmt.Errorf("%w: chunk-timeout", ErrNonPositive)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("%w: shutdown-timeout", ErrNonPositive)
	}
	if c.RetainCompleted < 0 {
		return fmt.Errorf("%w: retain-completed", ErrNonPositive)
	}
	if len(c.Targets) == 0 {
		return ErrEmptyTargets
	}
	return nil
}
