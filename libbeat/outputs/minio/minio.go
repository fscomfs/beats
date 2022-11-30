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
	"context"
	"fmt"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"
	c "github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/hashicorp/go-uuid"
	"github.com/minio/minio-go/v7"
	"github.com/spf13/cast"
	"os"
	"path"
	"path/filepath"
	"time"
)

func init() {
	outputs.RegisterType("minio", makeMinioOut)
}

type minioOutput struct {
	log      *logp.Logger
	filePath string
	beat     beat.Info
	observer outputs.Observer
	codec    codec.Codec
	stopChan chan bool
	files    map[string]*containLogFile
	client   *minio.Client
}

type containLogFile struct {
	file          *limitFile
	inputFileName string
	trackNo       string
	podName       string
	minioObjName  string
}

func (out *minioOutput) NewFile(inputFileName string, fileName string, trackNo string, podName string, minioObjName string, limitSize int64, limitLine int32) *containLogFile {
	limitFile, err := NewFile(fileName, limitLine, limitSize)
	if err != nil {
		fmt.Errorf("NewFile Openfile error:%+v", err)
		return nil
	}
	return &containLogFile{
		file:          limitFile,
		trackNo:       trackNo,
		podName:       podName,
		inputFileName: inputFileName,
		minioObjName:  minioObjName,
	}
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

	// disable bulk support in publisher pipeline
	_ = cfg.SetInt("bulk_max_size", -1, -1)

	fo := &minioOutput{
		log:      logp.NewLogger("file"),
		beat:     beat,
		observer: observer,
	}
	if err := fo.init(beat, config); err != nil {
		return outputs.Fail(err)
	}
	return outputs.Success(-1, 0, fo)
}

func (out *minioOutput) uploadMinio(limitFile *containLogFile, bucket string, objectName string) bool {
	ctx := context.Background()
	uuidR, _ := uuid.GenerateUUID()
	minioFileName := path.Join(out.filePath, uuidR)
	if limitFile.file.CopyFile(minioFileName) {
		uploadFile, err := os.Open(minioFileName)
		defer uploadFile.Close()
		fileStat, err := uploadFile.Stat()
		if err == nil {
			//var info minio.UploadInfo
			_, err = out.client.PutObject(ctx, bucket, objectName, uploadFile, fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err == nil {
				uploadFile.Close()
				os.Remove(minioFileName)
				return true
			} else {
				out.log.Errorf("upload minio error:%+v", objectName)
			}
		} else {
			out.log.Errorf("uploadMinio open file err:%+v", err)
		}

	}
	return false
}

func (out *minioOutput) init(beat beat.Info, c config) error {
	out.filePath = c.Path
	var err error
	out.files = make(map[string]*containLogFile)
	if err != nil {
		return err
	}

	os.MkdirAll(out.filePath, 7777)
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
			out.log.Errorf("make bucket err:%+v", err)
			fmt.Printf("make bucket err:%+v", err)
			panic("init minio error")
		}
	}
	ticker := time.NewTicker(1 * time.Second)
	go func(ticker *time.Ticker) {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				out.log.Infof("start upload minio")
				for s := range out.files {
					if _, err := os.Stat(s); err != nil {
						out.log.Infof("do start upload minio fileName=%v", s)
						success := out.uploadMinio(out.files[s], c.Bucket, out.files[s].minioObjName)
						if success {
							out.files[s].file.Remove()
							delete(out.files, s)
						}
					}
				}
			case stop := <-out.stopChan:
				if stop {
					out.log.Infof("stop upload minio")
					return
				}
			}
		}
	}(ticker)
	out.log.Infof("Initialized file output. "+
		"path=%v",
		out.filePath)
	return nil
}

// Implement Outputer
func (out *minioOutput) Close() error {
	for s := range out.files {
		out.files[s].file.Close()
	}
	out.stopChan <- true
	return nil
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
				out.log.Errorf("Failed to serialize the event: %+v", err)
			} else {
				out.log.Warnf("Failed to serialize the event: %+v", err)
			}
			out.log.Debugf("Failed event: %v", event)

			dropped++
			continue
		}
		logPath, err := event.Content.Fields.GetValue("log.file.path")
		trackNo, _ := event.Content.Fields.GetValue("trackNo")
		podName, _ := event.Content.Fields.GetValue("podName")
		minioObjName, _ := event.Content.Fields.GetValue("minioObjName")
		limitSize, _ := event.Content.Fields.GetValue("limitSize")
		limitLine, _ := event.Content.Fields.GetValue("limitLine")
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
		if _, ok := out.files[logPath.(string)]; !ok {
			if limitSize == nil {
				limitSize = 100 * 1024 * 1024
			}
			if limitLine == nil {
				limitLine = 10000000
			}
			out.files[logPath.(string)] = out.NewFile(logPath.(string), filepath.Join(out.filePath, inputFileName), trackNo.(string), podName.(string), minioObjName.(string), cast.ToInt64(limitSize), cast.ToInt32(limitLine))
		}
		outFile := out.files[logPath.(string)].file
		if _, err = outFile.Write(append(serializedEvent, '\n')); err != nil {
			st.WriteError(err)
			if event.Guaranteed() {
				out.log.Errorf("Writing event to file failed with: %+v", err)
			} else {
				out.log.Warnf("Writing event to file failed with: %+v", err)
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
	return "minio(" + out.filePath + ")"
}
