// Package fsutil은 여러 패키지가 공유하는 작은 파일시스템 헬퍼.
package fsutil

import "os"

// SyncDir은 디렉터리 entry 자체를 fsync한다. tmp+rename으로 atomic write을
// 끝낸 뒤 rename의 영속성을 확정하는 용도.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
