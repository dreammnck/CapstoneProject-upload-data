package services

import (
	"encoding/json"
	"fmt"
	"strings"

	elasticsearch8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type ElasticsearchService interface {
	UploadLog(index string, document interface{})
	Search(index string, query string) (map[string]interface{}, error)
}

type elasticsearchService struct {
	elasticClient *elasticsearch8.Client
}

func NewElasticsearchService(elasticClient *elasticsearch8.Client) ElasticsearchService {
	return elasticsearchService{elasticClient}
}

func (e elasticsearchService) UploadLog(index string, document interface{}) {
	_, err := e.elasticClient.Index(index, esutil.NewJSONReader(&document))

	if err != nil {
		fmt.Println(err.Error())
	}
}

func (e elasticsearchService) Search(index string, query string) (map[string]interface{}, error) {
	var b strings.Builder
	b.WriteString(query)
	read := strings.NewReader(b.String())

	response, err := e.elasticClient.Search(e.elasticClient.Search.WithIndex(index), e.elasticClient.Search.WithBody(read))

	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	json.NewDecoder(response.Body).Decode(&result)

	for _, hit := range result["hits"].(map[string]interface{})["hits"].([]interface{}) {
		return hit.(map[string]interface{})["_source"].(map[string]interface{}), nil
	}

	return nil, nil

}
