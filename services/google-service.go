package services

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"
	"upload-data/types"

	"cloud.google.com/go/storage"
)

type googleStorageService struct {
	storageClient *storage.Client
}

type GoogleStorageService interface {
	UploadCsv(uploadDataParam types.UploadDataParam) error
}

func NewGoogleStorageService(storageClient *storage.Client) GoogleStorageService {
	return googleStorageService{storageClient}
}

func (g googleStorageService) UploadCsv(uploadDataParam types.UploadDataParam) error {
	file, err := os.Open(uploadDataParam.Filename)

	defer file.Close()

	if err != nil {
		return err
	}

	year, month, day := time.Now().Date()
	sw := g.storageClient.Bucket(uploadDataParam.BucketName).Object(fmt.Sprintf("%v/%v/%v/%v/%v/%v", uploadDataParam.ModelName,
	 uploadDataParam.DeviceId, year, int(month), day, uploadDataParam.Filename)).NewWriter(context.Background())
	sw.ContentType = "text/csv"

	if _, err := io.Copy(sw, file); err != nil {
		fmt.Printf("%v\n", err)
		return err
	}

	if err := sw.Close(); err != nil {
		fmt.Printf("%v", err.Error())
		return err
	}

	defer os.Remove(uploadDataParam.Filename)
	return nil
}
