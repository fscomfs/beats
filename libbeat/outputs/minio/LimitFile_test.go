package minio

import (
	"testing"
)

func TestFile(t *testing.T) {
	limitFile, e := NewFile("/tmp/test.log", 1000)
	if e != nil {

	}

	limitFile.CopyFile("/tmp/test2.log")
}
