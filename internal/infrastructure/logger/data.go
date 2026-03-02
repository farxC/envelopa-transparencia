package logger

import "sync"

// Logger provides structured logging with levels

type Logger struct {
	MinLevel LogLevel
	mu       sync.Mutex
}

// LogLevel represents the severity of a log message
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)
