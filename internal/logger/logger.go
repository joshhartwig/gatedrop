package logger

import (
	"log/slog"
	"os"

	"github.com/lmittmann/tint"
)

func New(env string) *slog.Logger {

	if env == "development" {
		return slog.New(tint.NewHandler(os.Stdout, &tint.Options{
			Level:     slog.LevelDebug,
			AddSource: true,
		}))
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}
