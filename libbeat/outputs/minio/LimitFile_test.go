package minio

import (
	"os"
	"testing"
)

func TestFile(t *testing.T) {
	f, _ := os.OpenFile("/log/test1.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	i2 := 2600

	buf := make([]byte, i2, i2)
	for i := 0; i < i2; i++ {
		buf[i] = 56
	}
	buf[0] = 33
	buf[i2-2] = 36
	buf[i2-1] = '\n'
	f.Write(buf)
}
