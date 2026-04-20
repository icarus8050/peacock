package wal

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var ErrClosed = errors.New("wal: closed")

type WAL struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	closed bool
}

func Open(opts Options) (*WAL, error) {
	if err := os.MkdirAll(opts.DirPath, 0755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	fileName := opts.FileName
	if fileName == "" {
		fileName = "wal.log"
	}
	path := filepath.Join(opts.DirPath, fileName)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}

	bufSize := opts.BufferSize
	if bufSize <= 0 {
		bufSize = 32 * 1024
	}

	return &WAL{
		file:   file,
		writer: bufio.NewWriterSize(file, bufSize),
	}, nil
}

func (w *WAL) Append(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	data := entry.Encode()
	_, err := w.writer.Write(data)
	return err
}

// Sync flushes the buffer and fsyncs the underlying file.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	return nil
}

// Close flushes, syncs, and closes the underlying file. Callers that run
// a background Syncer against this WAL must Stop() it before calling Close.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}
	w.closed = true

	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		return fmt.Errorf("wal: flush on close: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		w.file.Close()
		return fmt.Errorf("wal: fsync on close: %w", err)
	}
	return w.file.Close()
}

func (w *WAL) Path() string {
	return w.file.Name()
}
