package minio

import (
	"fmt"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func NewMinioClient(config *config) (client *minio.Client) {
	minioClient, err := minio.New(config.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.UserName, config.Password, ""),
		Secure: false,
	})
	if err != nil {
		fmt.Printf("create minio client error:%+v", err)
		return nil
	}
	return minioClient
}
