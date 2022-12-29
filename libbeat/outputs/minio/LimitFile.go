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
	MinioObjName string `json:"minio_obj_name"`
	Size         int64  `json:"size"`
	MaxSizeBytes uint   `json:"max_size_bytes"`
	LimitLine    int32  `json:"limit_line"`
	LimitSize    int64  `json:"limit_size"`
	mutex        sync.Mutex
	Position     int64 `json:"position"`
	RotatorNum   int   `json:"rotator_num"`
	RotatorSize  int64 `json:"rotator_size"`
}

func NewFile(fileName string, limitLine int32, limitSize int64) (file *LimitFile, err error) {
	if limitLine == 0 {
		limitLine = 10000
	}
	if limitSize == 0 {
		limitSize = 100 * 1024 * 1024
	}
	if file, err := os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600); err == nil {
		return &LimitFile{
			file:         file,
			FileName:     fileName,
			Size:         0,
			MaxSizeBytes: 10 * 1024 * 1024,
			LimitLine:    limitLine,
			LimitSize:    limitSize,
			Position:     int64(0),
			RotatorNum:   int(0),
			RotatorSize:  int64(0),
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
		l.file.Close()
		os.Remove(l.FileName)
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
	if int64(dataLen)+l.Size > l.LimitSize {
		l.file.Write(make([]byte, l.LimitSize-l.Position, l.LimitSize-l.Position))
		l.file.Seek(0, 0)
		l.RotatorSize = l.Position
		l.Position = 0
		l.RotatorNum += 1
		l.Size = 0
	}
	l.file.Write(data)
	l.Position += int64(dataLen)
	l.Size += int64(dataLen)
	return int(dataLen), nil
}

func (l *LimitFile) CopyFile(toFileName string) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	file, err := os.OpenFile(l.FileName, os.O_RDONLY, 0660)
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
	if l.RotatorNum > 0 {
		isStart = false
		rotatorSize = l.Position
		file.Seek(l.Position, 0)
	}
	//
	linNum := int32(0)
	for {
		line, err := reader.ReadBytes('\n')
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
		if err == io.EOF || rotatorSize > l.RotatorSize {
			if !isStart {
				file.Seek(0, 0) //reset position
				isStart = true
				continue
			}
		}
		if line[0] == 0 {
			continue
		}
		if l.LimitLine <= linNum+1 {
			return true
		}
		if isStart {
			position += int64(len(line))
		}
		writer.Write(line)
		if position >= l.Position {
			return true
		}
	}
}
