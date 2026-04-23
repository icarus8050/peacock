package wal

import "path/filepath"

const (
	defaultFileName   = "wal.log"
	defaultBufferSize = 32 * 1024
)

type Options struct {
	DirPath    string
	FileName   string
	BufferSize int
}

func DefaultOptions(dirPath string) Options {
	return Options{
		DirPath:    dirPath,
		FileName:   defaultFileName,
		BufferSize: defaultBufferSize,
	}
}

func (o Options) withDefaults() Options {
	if o.FileName == "" {
		o.FileName = defaultFileName
	}
	if o.BufferSize <= 0 {
		o.BufferSize = defaultBufferSize
	}
	return o
}

func (o Options) path() string {
	return filepath.Join(o.DirPath, o.FileName)
}
