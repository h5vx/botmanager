// Package observability содержит то, что относится к "как узнать, что
// сервис жив и что в нём происходит": структурированные логи и HTTP-ручки
// /healthz, /readyz, /metrics.
package observability

import (
	"log/slog"
	"os"
	"strings"
)

// LogFields — обязательные поля каждой записи лога, общие для всех событий
// процесса (service, version присутствуют всегда; trace_id
// добавится, когда появится трассировка).
type LogFields struct {
	Service string
	Version string
}

// NewLogger создаёт slog.Logger с JSON-обработчиком, пишущий в stdout —
// единственный canonical sink логов (только stdout/stderr, никаких
// файлов). Уровень берётся из levelName (DEBUG/INFO/WARNING/ERROR/
// CRITICAL); неизвестное значение тихо трактуется как INFO.
//
// Поля service и version добавляются ко всем записям через slog.Logger.With,
// так что каждый вызов log.Info(...) уже несёт их без явного указания на
// месте вызова.
func NewLogger(fields LogFields, levelName string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     parseLevel(levelName),
		AddSource: false,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// slog по умолчанию называет поле времени "time" и уровня "level";
			// формат логов требует "ts". Уровень уже называется "level".
			if a.Key == slog.TimeKey && len(groups) == 0 {
				a.Key = "ts"
			}
			return a
		},
	})

	logger := slog.New(handler).With(
		slog.String("service", fields.Service),
		slog.String("version", fields.Version),
	)
	return logger
}

// parseLevel переводит уровни конфигурации (DEBUG, INFO,
// WARNING, ERROR, CRITICAL) в slog.Level. У slog нет отдельного уровня
// CRITICAL — он маппится на самый высокий доступный (ERROR+4), что в выводе
// остаётся различимым числовым значением.
func parseLevel(name string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO", "":
		return slog.LevelInfo
	case "WARNING", "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	case "CRITICAL":
		return slog.LevelError + 4
	default:
		return slog.LevelInfo
	}
}
