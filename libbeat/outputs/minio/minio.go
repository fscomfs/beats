// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package minio

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"
	c "github.com/elastic/elastic-agent-libs/config"
	"github.com/hashicorp/go-uuid"
	"github.com/minio/minio-go/v7"
	"github.com/spf13/cast"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

func init() {
	outputs.RegisterType("minio", makeMinioOut)
}

type minioOutput struct {
	outPath  string
	beat     beat.Info
	observer outputs.Observer
	codec    codec.Codec
	stopChan chan bool
	Files    map[string]*ContainLogFile
	client   *minio.Client
	mutex    sync.Mutex
	config   *config
}

type ContainLogFile struct {
	File          *LimitFile `json:"file"`
	InputFileName string     `json:"input_fileName"`
	TrackNo       string     `json:"track_no"`
	PodName       string     `json:"pod_name"`
	MinioObjName  string     `json:"minio_obj_name"`
	RemovedFlag   bool       `json:"removed_flag"`
	UpdateFlag    bool       `json:"update_flag"`
	RemovedTime   time.Time  `json:"removed_time"`
}

// makeMinioOut instantiates a new file output instance.
func makeMinioOut(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *c.C,
) (outputs.Group, error) {
	config := defaultConfig()
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}
	fmt.Printf("make Minio out start")
	// disable bulk support in publisher pipeline
	_ = cfg.SetInt("bulk_max_size", -1, -1)
	os.MkdirAll("/out/filebeat/log/", 0644)
	logFile, _ := os.OpenFile("/out/filebeat/log/all.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	log.SetOutput(logFile)
	fo := &minioOutput{
		beat:     beat,
		observer: observer,
		config:   &config,
	}
	if err := fo.minioOutInit(beat, config); err != nil {
		return outputs.Fail(err)
	}
	fo.ApiInit()
	return outputs.Success(-1, 0, fo)
}

func (out *minioOutput) NewFile(inputFileName string, fileName string, trackNo string, podName string, minioObjName string, limitSize int64) {
	log.Printf("new Log file podName=%+v,minioObjName=%+v,inputFileName=%+v", podName, minioObjName, inputFileName)
	limitFile, err := NewFile(fileName, limitSize)
	if err != nil {
		fmt.Errorf("NewFile Openfile error:%+v", err)
	}
	out.Files[inputFileName] = &ContainLogFile{
		File:          limitFile,
		TrackNo:       trackNo,
		PodName:       podName,
		InputFileName: inputFileName,
		MinioObjName:  minioObjName,
		RemovedFlag:   false,
		UpdateFlag:    true,
	}

}
func (out *minioOutput) uploadMinio(containLogFile *ContainLogFile, bucket string) bool {
	ctx := context.Background()
	uuidR, _ := uuid.GenerateUUID()
	minioFileName := path.Join(out.outPath, uuidR+"_tmp")
	defer func() {
		if err := recover(); err != nil {
			log.Printf("uploadMinio error 002")
		}
	}()
	defer os.Remove(minioFileName)
	if containLogFile.File.CopyFile(minioFileName) {
		uploadFile, err := os.Open(minioFileName)
		defer uploadFile.Close()
		fileStat, err := uploadFile.Stat()
		if err == nil {
			_, err = out.client.PutObject(ctx, bucket, containLogFile.MinioObjName, uploadFile, fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err == nil {
				log.Printf("log upload minio success podName=%+v,minioObjName=%+v", containLogFile.PodName, containLogFile.MinioObjName)
				return true
			} else {
				log.Printf("upload minio error:%+v", containLogFile.MinioObjName)
			}
		} else {
			log.Printf("uploadMinio open file err:%+v", err)
		}

	}
	return false
}

func (out *minioOutput) minioOutInit(beat beat.Info, c config) error {
	out.outPath = c.Path
	var err error
	out.Files = make(map[string]*ContainLogFile)
	if err != nil {
		return err
	}

	os.MkdirAll(out.outPath, 7777)
	out.codec, err = codec.CreateEncoder(beat, c.Codec)
	if err != nil {
		return err
	}
	//init ticker
	out.stopChan = make(chan bool)
	out.client = NewMinioClient(&c)
	//bucket
	if exists, _ := out.client.BucketExists(context.Background(), c.Bucket); !exists {
		err = out.client.MakeBucket(context.Background(), c.Bucket, minio.MakeBucketOptions{
			ObjectLocking: false,
		})
		if err != nil {
			log.Printf("make bucket err:%+v", err)
			panic("init minio error")
		}
	}
	out.UnMarshal()
	ticker := time.NewTicker(3 * time.Second)
	go func(ticker *time.Ticker) {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				out.UpLoad(c)
			case stop := <-out.stopChan:
				if stop {
					fmt.Printf("stop upload minio")
					return
				}
			}
		}
	}(ticker)
	log.Printf("Initialized file output. "+
		"path=%+v",
		out.outPath)
	return nil
}

func (out *minioOutput) ApiInit() {
	socketPath := "/run/filebeat_minio.sock"
	os.Remove(socketPath)
	socket, err := net.Listen("unix", socketPath)
	if err == nil {
		log.Printf("api listen path=%+v", socketPath)
		go func() {
			m := http.NewServeMux()
			m.HandleFunc("/uploadFile", func(w http.ResponseWriter, r *http.Request) {
				trackNo := r.URL.Query().Get("trackNo")
				log.Printf("api uploadFile trackNo=%+v", trackNo)
				if out.UploadByTrackNo(trackNo) {
					w.Write([]byte("1"))
				} else {
					w.Write([]byte("0"))
				}
				w.WriteHeader(http.StatusOK)
			})
			server := http.Server{
				Handler: m,
			}
			if err := server.Serve(socket); err != nil {
				log.Print(err)
			}
		}()
	} else {
		log.Printf("api init error %+v", err.Error())
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		os.Remove(socketPath)
		os.Exit(1)
	}()

}

func (out *minioOutput) UploadByTrackNo(trackNo string) bool {
	out.mutex.Lock()
	defer out.mutex.Unlock()
	for s := range out.Files {
		if out.Files[s].TrackNo == trackNo {
			success := out.uploadMinio(out.Files[s], out.config.Bucket)
			if success {
				return true
			} else {
				return false
			}
		}
	}
	return false
}

func (out *minioOutput) UpLoad(c config) {
	out.mutex.Lock()
	defer out.mutex.Unlock()
	out.marshal()
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Upload panic err %+v", err)
		}
	}()
	for s := range out.Files {
		if _, err := os.Stat(s); err != nil {
			if !out.Files[s].RemovedFlag && os.IsNotExist(err) {
				log.Printf("do upload minio start pod name=%+v,fileName=%+v", out.Files[s].PodName, s)
				success := out.uploadMinio(out.Files[s], c.Bucket)
				if success {
					out.RemoveMark(s)
				}
			} else {
				if out.Files[s].RemovedFlag && out.Files[s].RemovedTime.UnixMilli()+600*1000 <= time.Now().UnixMilli() {
					out.RemoveFile(s)
				}
			}

		}
	}
}

func (out *minioOutput) Close() error {
	defer func() {
		if error := recover(); error != nil {
			fmt.Printf("Close file error: %+v", error)
		}
	}()
	for s := range out.Files {
		out.Files[s].File.Close()
	}
	out.stopChan <- true
	return nil
}
func (out *minioOutput) RemoveMark(fileName string) {
	out.mutex.Lock()
	defer out.mutex.Unlock()
	defer func() {
		if error := recover(); error != nil {
			fmt.Printf("Remove mark error: %+v", error)
		}
	}()
	if _, ok := out.Files[fileName]; ok && !out.Files[fileName].RemovedFlag {
		log.Printf("remove mark PodName=%+v", out.Files[fileName].PodName)
		out.Files[fileName].RemovedFlag = true
		out.Files[fileName].UpdateFlag = false
		out.Files[fileName].RemovedTime = time.Now()
	}
}

func (out *minioOutput) RemoveFile(fileName string) {
	out.mutex.Lock()
	defer out.mutex.Unlock()
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Remove file error: %+v", err)
		}
	}()
	if _, ok := out.Files[fileName]; ok {
		log.Printf("remove file PodName=%+v", out.Files[fileName].PodName)
		out.Files[fileName].File.Close()
		delete(out.Files, fileName)
	}
}

func (out *minioOutput) Write(inputFileName string, data []byte) (int, error) {
	if o, ok := out.Files[inputFileName]; ok {
		o.UpdateFlag = true
		return o.File.Write(data)
	}
	return 0, nil
}

func (out *minioOutput) Publish(_ context.Context, batch publisher.Batch) error {
	defer batch.ACK()
	st := out.observer
	events := batch.Events()
	st.NewBatch(len(events))
	dropped := 0
	for i := range events {
		event := &events[i]
		serializedEvent, err := out.codec.Encode(out.beat.Beat, &event.Content)
		if err != nil {
			if event.Guaranteed() {
				log.Printf("Failed to serialize the event: %+v", err)
			} else {
				log.Printf("Failed to serialize the event: %+v", err)
			}
			log.Printf("Failed event: %v", event)

			dropped++
			continue
		}
		contentStr, err := event.Content.Fields.GetValue("message")
		var c []byte
		if contentStr != nil {
			c = []byte(contentStr.(string))
		}
		logPath, err := event.Content.Fields.GetValue("log.file.path")
		trackNo, _ := event.Content.Fields.GetValue("trackNo")
		podName, _ := event.Content.Fields.GetValue("podName")
		minioObjName, _ := event.Content.Fields.GetValue("minioObjName")
		limitSize, _ := event.Content.Fields.GetValue("limitSize")
		if minioObjName == nil {
			minioObjName = "pod-json.log"
		}
		if podName == nil {
			podName = "k8s"
		}
		if trackNo == nil {
			trackNo = "logPath"
		}
		inputFileName := path.Base(logPath.(string))
		if _, ok := out.Files[logPath.(string)]; !ok {
			if limitSize == nil {
				limitSize = 100 * 1024 * 1024
			}
			out.NewFile(logPath.(string), filepath.Join(out.outPath, inputFileName), trackNo.(string), podName.(string), minioObjName.(string), cast.ToInt64(limitSize))
		}
		if _, err = out.Write(logPath.(string), append(c, '\n')); err != nil {
			st.WriteError(err)
			if event.Guaranteed() {
				log.Printf("Writing event to file failed with: %+v", err)
			} else {
				log.Printf("Writing event to file failed with: %+v", err)
			}
			dropped++
			continue
		}
		st.WriteBytes(len(serializedEvent) + 1)
	}
	st.Dropped(dropped)
	st.Acked(len(events) - dropped)
	return nil
}

func (out *minioOutput) String() string {
	return "minio(" + out.outPath + ")"
}

func (out *minioOutput) marshal() {
	//创建缓存
	buf := new(bytes.Buffer)
	//把指针丢进去
	enc := gob.NewEncoder(buf)
	err := enc.Encode(out.Files)
	if err != nil {
		return
	}
	if err := enc.Encode(out.Files); err == nil {
		os.WriteFile(path.Join(out.outPath, "registry.log"), buf.Bytes(), 0644)
	}
}

func (out *minioOutput) UnMarshal() {
	_, err := os.Stat(path.Join(out.outPath, "registry.log"))
	if err != nil {
		return
	}
	f, err := os.Open(path.Join(out.outPath, "registry.log"))
	defer f.Close()
	if err != nil {
		return
	}
	content, err := ioutil.ReadAll(f)
	if err != nil {
		return
	}
	//创建缓存
	decode := gob.NewDecoder(bytes.NewBuffer(content))
	if err != nil {
		return
	}
	containLogFile := make(map[string]*ContainLogFile)
	decode.Decode(&containLogFile)
	out.Files = containLogFile
	for k, v := range out.Files {
		if out.Files[k].File.file == nil {
			f, err = os.OpenFile(v.File.FileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err == nil {
				out.Files[k].File.file = f
			} else {
				delete(out.Files, k)
			}
		}
	}
}
