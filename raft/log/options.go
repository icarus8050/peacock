package raftlog

const (
	defaultBufferSize     = 32 * 1024
	defaultMaxSegmentSize = 64 * 1024 * 1024
)

type Options struct {
	DirPath        string
	BufferSize     int
	MaxSegmentSize int64
}

func DefaultOptions(dirPath string) Options {
	return Options{
		DirPath:        dirPath,
		BufferSize:     defaultBufferSize,
		MaxSegmentSize: defaultMaxSegmentSize,
	}
}

func (o Options) withDefaults() Options {
	if o.BufferSize <= 0 {
		o.BufferSize = defaultBufferSize
	}
	if o.MaxSegmentSize <= 0 {
		o.MaxSegmentSize = defaultMaxSegmentSize
	}
	return o
}
