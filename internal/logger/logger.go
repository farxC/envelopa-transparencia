package logger

import (
	"fmt"
	"log"
	"os"
	"time"
)

var logLevelNames = map[LogLevel]string{
	LevelDebug: "DEBUG",
	LevelInfo:  "INFO",
	LevelWarn:  "WARN",
	LevelError: "ERROR",
}

// SetLogLevel sets the minimum log level
func (l *Logger) SetLogLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.MinLevel = level
}

func (l *Logger) log(level LogLevel, component, message string, args ...interface{}) {
	if level < l.MinLevel {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	levelStr := logLevelNames[level]
	formattedMsg := fmt.Sprintf(message, args...)

	l.mu.Lock()
	defer l.mu.Unlock()

	if component != "" {
		log.Printf("[%s] [%s] [%s] %s", timestamp, levelStr, component, formattedMsg)
	} else {
		log.Printf("[%s] [%s] %s", timestamp, levelStr, formattedMsg)
	}
}

// Debug logs a debug message
func (l *Logger) Debug(component, message string, args ...interface{}) {
	l.log(LevelDebug, component, message, args...)
}

// Info logs an info message
func (l *Logger) Info(component, message string, args ...interface{}) {
	l.log(LevelInfo, component, message, args...)
}

// Warn logs a warning message
func (l *Logger) Warn(component, message string, args ...interface{}) {
	l.log(LevelWarn, component, message, args...)
}

// Error logs an error message
func (l *Logger) Error(component, message string, args ...interface{}) {
	l.log(LevelError, component, message, args...)
}

// Fatal logs an error message and exits
func (l *Logger) Fatal(component, message string, args ...interface{}) {
	l.log(LevelError, component, message, args...)
	os.Exit(1)
}
