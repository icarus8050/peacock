package wal

type Options struct {
	DirPath    string
	FileName   string
	BufferSize int
}

func DefaultOptions(dirPath string) Options {
	return Options{
		DirPath:    dirPath,
		FileName:   "wal.log",
		BufferSize: 32 * 1024,
	}
}
