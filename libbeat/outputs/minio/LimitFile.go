package minio

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

type limitFile struct {
	file         *os.File
	fileName     string
	minioObjName string
	size         int64
	maxSizeBytes uint
	limitLine    int32
	limitSize    int64
	mutex        sync.Mutex
	position     int64 //
	rotatorNum   int
	rotatorSize  int64
}

func NewFile(fileName string, limitLine int32, limitSize int64) (file *limitFile, err error) {
	if limitLine == 0 {
		limitLine = 10000
	}
	if limitSize == 0 {
		limitSize = 100 * 1024 * 1024
	}
	if file, err := os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600); err == nil {
		return &limitFile{
			file:         file,
			fileName:     fileName,
			size:         0,
			maxSizeBytes: 10 * 1024 * 1024,
			limitLine:    limitLine,
			limitSize:    limitSize,
			position:     int64(0),
			rotatorNum:   int(0),
			rotatorSize:  int64(0),
		}, nil
	} else {
		return nil, err
	}

}

func (l *limitFile) Close() {
	l.file.Close()
}

func (l *limitFile) Remove() {
	if _, err := l.file.Stat(); err == nil {
		l.file.Close()
		os.Remove(l.fileName)
	}
}

func (l *limitFile) Write(data []byte) (int, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	dataLen := uint(len(data))
	if dataLen > l.maxSizeBytes {
		return 0, fmt.Errorf("data size (%d bytes) is greater than "+
			"the max file size (%d bytes)", dataLen, l.maxSizeBytes)
	}
	if int64(dataLen)+l.size > l.limitSize {
		l.file.Seek(0, 0)
		l.rotatorSize = l.position
		l.position = 0
		l.rotatorNum += 1
		l.size = 0
	}
	l.file.Write(data)
	l.position += int64(dataLen)
	l.size += int64(dataLen)
	return int(dataLen), nil
}

func (l *limitFile) CopyFile(toFileName string) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	file, err := os.OpenFile(l.fileName, os.O_RDONLY, 0660)
	defer file.Close()
	toFile, err2 := os.OpenFile(toFileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0660)
	defer toFile.Close()
	if err != nil && err2 != nil {

	}
	writer := bufio.NewWriter(toFile)
	defer writer.Flush()
	reader := bufio.NewReader(file)
	isStart := true
	position := int64(0)
	rotatorSize := int64(0)
	if l.rotatorNum > 0 {
		isStart = false
		rotatorSize = l.position
		file.Seek(l.position, 0)
	}
	//
	linNum := int32(0)
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return false
		}
		linNum++
		if !isStart {
			rotatorSize += int64(len(line))
		}
		if linNum == 1 && !isStart {
			continue
		}
		if err == io.EOF || rotatorSize > l.rotatorSize {
			if !isStart {
				file.Seek(0, 0) //reset position
				isStart = true
				continue
			}
		}
		if l.limitLine <= linNum+1 {
			return true
		}
		if isStart {
			position += int64(len(line))
		}
		writer.WriteString(line)
		if position >= l.position {
			return true
		}
	}
}
