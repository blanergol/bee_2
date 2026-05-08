// Package obs содержит конфигурацию структурированного логирования сервиса
// purged. Используется log/slog с JSON-обработчиком.
package obs

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// NewLogger возвращает JSON-логгер log/slog, пишущий в w (или os.Stdout, если nil),
// с уровнем level (info|debug|warn|error; пустая строка == info).
func NewLogger(w io.Writer, level string) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	lvl := parseLevel(level)
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	return slog.New(h)
}

func parseLevel(s string) slog.Leveler {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "", "info":
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}
