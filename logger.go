package pg

import (
	"log"
)

// Logger is implemented by any logging system that is used for standard logs.
type Logger interface {
	Error(err error)
	Info(string, ...interface{})
	Warn(string, ...interface{})
}

func (d *Database) SetLogger(logger Logger) {
	d.logger = logger
}

type defaultLog struct {
	*log.Logger
}

func defaultLogger() *defaultLog {
	return &defaultLog{Logger: log.Default()}
}

func (l *defaultLog) Error(err error) {
	l.Printf("ERROR: [pg] %v", err)
}

func (l *defaultLog) Info(f string, v ...interface{}) {
	l.Printf("INFO: [pg] "+f, v...)
}

func (l *defaultLog) Warn(f string, v ...interface{}) {
	l.Printf("WARN: [pg] "+f, v...)
}
