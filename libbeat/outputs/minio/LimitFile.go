package minio

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/spf13/cast"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
)

type LimitFile struct {
	file         *os.File
	FileName     string `json:"file_name"`
	MaxSizeBytes int    `json:"max_size_bytes"`
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
			MaxSizeBytes: 4 * 1024,
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

func (l *LimitFile) Write(data []byte) (ret int, re error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	dataLen := len(data)
	defer func() {
		if e := recover(); e != nil {
			ret = dataLen
		}
	}()
	var j interface{}
	e := json.Unmarshal(data, &j)
	if e != nil {
		l.file.Write(l.LineConfound(data))
	}
	jsonData := j.(map[string]interface{})
	if logContent, ok := jsonData["log"]; ok {
		l.file.Write(l.LineConfound([]byte(cast.ToString(logContent))))

	}
	return dataLen, nil
}

func (l *LimitFile) CopyFile(toFileName string, appendMessage string) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	defer func() {
		if err := recover(); err != nil {
			log.Printf("CopyFile error %+v", err)
		}
	}()
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
	if fileSize > l.LimitSize {
		seek = fileSize - l.LimitSize
	}
	file.Seek(seek, 0)
	writer := bufio.NewWriter(toFile)
	defer writer.Flush()
	reader := bufio.NewReader(file)
	if seek > 0 {
		lineNum := 0
		for {
			if line, err := reader.ReadBytes('\n'); err == nil {
				lineNum++
				if lineNum == 1 {
					continue
				}
				writer.Write(line)
			} else {
				if err == io.EOF {
					break
				} else {
					break
				}
			}
		}
	} else {
		io.Copy(writer, reader)
	}

	if appendMessage != "" {
		log.Printf("Copy file success append message %+v", appendMessage)
		writer.Write([]byte("\n" + appendMessage + "\n"))
		writer.Flush()
	}
	return true
}

var zz = "#^()abc=defg123898TUVWXYZ*<>_"
var zzLen = len(zz)

func (l *LimitFile) LineConfound(line []byte) []byte {
	rIndex := bytes.LastIndexByte(line, '\r')
	if rIndex > 0 && (rIndex+3) <= len(line) {
		line = line[rIndex+1:]
	}
	lens := len(line)
	if lens > l.MaxSizeBytes {
		for i := 0; i < lens/600; i++ {
			s := rand.Intn(zzLen - 1)
			is := rand.Intn(lens - 2)
			line = append(line[:is+1], append([]byte{zz[s]}, line[is+1:]...)...)
		}
	}
	return line
}
