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
	"fmt"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
)

type config struct {
	Path        string       `config:"path"`
	Filename    string       `config:"filename"`
	EndPoint    string       `config:"EndPoint"`
	Bucket      string       `config:"Bucket"`
	UserName    string       `config:"UserName"`
	Password    string       `config:"Password"`
	Codec       codec.Config `config:"codec"`
	Permissions uint32       `config:"permissions"`
}

func defaultConfig() config {
	return config{
		Path:        "/filebeat",
		Permissions: 0600,
	}
}

func (c *config) Validate() error {
	if c.EndPoint == "" {
		return fmt.Errorf("minio EndPoint can not empty")
	}
	if c.Bucket == "" {
		return fmt.Errorf("minio Bucket can not empty")
	}
	if c.UserName == "" {
		return fmt.Errorf("minio UserName can not empty")
	}
	if c.Password == "" {
		return fmt.Errorf("minio Password can not empty")
	}

	return nil
}
