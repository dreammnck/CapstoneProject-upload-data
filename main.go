package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"
	"upload-data/services"
	"upload-data/tasks"

	"cloud.google.com/go/storage"
	elasticsearch8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/go-co-op/gocron"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/api/option"
)

func InitGoogleStorageClient() *storage.Client {

	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile(viper.GetString("googleStorage.credencialFilePath")))
	if err != nil {
		panic(err)
	}

	return client

}
func InitElasticsearch() *elasticsearch8.Client {
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	certs, err := ioutil.ReadFile(viper.GetString("elasticsearch.caCert"))

	if err != nil {
		fmt.Println("error read file")
	}

	rootCAs.AppendCertsFromPEM(certs)
	config := elasticsearch8.Config{
		Addresses: viper.GetStringSlice("elasticsearch.address"),
		Username:  viper.GetString("elasticsearch.username"),
		Password:  viper.GetString("elasticsearch.password"),
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    rootCAs,
			},
		},
	}

	client, err := elasticsearch8.NewClient(config)

	if err != nil {
		panic(err)
	}

	return client

}

func InitViper() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
}

func InitMongoClient() *mongo.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(viper.GetString("mongo.url")))
	if err != nil {
		panic(err)
	}

	fmt.Println("Connecting to mongo database ....")

	return client
}

func main() {
	InitViper()
	s := gocron.NewScheduler(time.UTC)
	googleStorageClient := InitGoogleStorageClient()
	elasticsearchClient := InitElasticsearch()
	mongoClient := InitMongoClient()

	googleService := services.NewGoogleStorageService(googleStorageClient)
	elasticsearchService := services.NewElasticsearchService(elasticsearchClient)

	uploadedDataTask := tasks.NewUploadDataTask(mongoClient, googleService, elasticsearchService)

	s.Every(20 * time.Second).Do(uploadedDataTask.Upload)
	s.StartAsync()

	for {

	}
}
