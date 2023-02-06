package minio

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

type LimitFile struct {
	file         *os.File
	FileName     string `json:"file_name"`
	MaxSizeBytes uint   `json:"max_size_bytes"`
	LimitSize    int64  `json:"limit_size"`
	mutex        sync.Mutex
}

func NewFile(fileName string, limitSize int64) (file *LimitFile, err error) {
	if limitSize == 0 {
		limitSize = 100 * 1024 * 1024
	}
	if file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
		return &LimitFile{
			file:         file,
			FileName:     fileName,
			MaxSizeBytes: 10 * 1024 * 1024,
			LimitSize:    limitSize,
		}, nil
	} else {
		return nil, err
	}

}

func (l *LimitFile) Close() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.file.Close()
}

func (l *LimitFile) Remove() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if _, err := l.file.Stat(); err == nil {
		if !os.IsNotExist(err) {
			l.file.Close()
			os.Remove(l.FileName)
		}
	}
}

func (l *LimitFile) Write(data []byte) (int, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	dataLen := uint(len(data))
	if dataLen > l.MaxSizeBytes {
		return 0, fmt.Errorf("data size (%d bytes) is greater than "+
			"the max file size (%d bytes)", dataLen, l.MaxSizeBytes)
	}
	l.file.Write(data)
	return int(dataLen), nil
}

func (l *LimitFile) CopyFile(toFileName string) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	file, err := os.OpenFile(l.FileName, os.O_RDONLY, 0644)
	defer file.Close()
	var fileSize int64 = 0
	if fileInfo, error := file.Stat(); error == nil {
		fileSize = fileInfo.Size()
	}

	toFile, err2 := os.OpenFile(toFileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	defer toFile.Close()
	if err != nil || err2 != nil {
		return false
	}
	var seek int64 = 0
	var firstIgnore = false
	if fileSize > l.LimitSize {
		firstIgnore = true
		seek = fileSize - l.LimitSize
	}
	file.Seek(seek, 0)
	writer := bufio.NewWriter(toFile)
	defer writer.Flush()
	reader := bufio.NewReader(file)
	lineNum := 0
	for {
		if line, err := reader.ReadBytes('\n'); err == nil {
			lineNum++
			if firstIgnore && lineNum == 1 {
				continue
			}
			writer.Write(line)
		} else {
			if err == io.EOF {
				return true
			} else {
				return false
			}
		}
	}

}
