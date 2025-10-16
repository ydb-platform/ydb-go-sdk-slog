package slog

import (
	"context"
	"log/slog"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

var _ log.Logger = adapter{}

type adapter struct {
	l *slog.Logger
}

func (a adapter) Log(ctx context.Context, msg string, fields ...log.Field) {
	l := a.l

	for _, name := range log.NamesFromContext(ctx) {
		l = l.WithGroup(name)
	}

	l.LogAttrs(ctx, Level(ctx), msg, Fields(append(log.FieldsFromContext(ctx), fields...))...)
}

func fieldToAttr(field log.Field) slog.Attr {
	switch field.Type() {
	case log.IntType:
		return slog.Int(field.Key(), field.IntValue())
	case log.Int64Type:
		return slog.Int64(field.Key(), field.Int64Value())
	case log.StringType:
		return slog.String(field.Key(), field.StringValue())
	case log.BoolType:
		return slog.Bool(field.Key(), field.BoolValue())
	case log.DurationType:
		return slog.Duration(field.Key(), field.DurationValue())
	case log.StringsType:
		return slog.Any(field.Key(), field.StringsValue())
	case log.ErrorType:
		return slog.Any("error", field.ErrorValue())
	case log.StringerType:
		return slog.Any(field.Key(), field.Stringer())
	default:
		return slog.Any(field.Key(), field.AnyValue())
	}
}

func Fields(fields []log.Field) []slog.Attr {
	attrs := make([]slog.Attr, len(fields))
	for i, f := range fields {
		attrs[i] = fieldToAttr(f)
	}

	return attrs
}

func Level(ctx context.Context) slog.Level {
	switch log.LevelFromContext(ctx) {
	case log.TRACE, log.DEBUG:
		return slog.LevelDebug
	case log.INFO:
		return slog.LevelInfo
	case log.WARN:
		return slog.LevelWarn
	case log.ERROR:
		return slog.LevelError
	case log.FATAL:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type Option = log.Option

func WithLogQuery() Option {
	return log.WithLogQuery()
}
