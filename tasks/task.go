package tasks

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
	services "upload-data/services"
	"upload-data/types"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const MAX_CONCURRENT_JOBS = 2

type UploadDataTask interface {
	Upload()
}

type uploadDataTask struct {
	medicalModelCollection *mongo.Collection
	medicalDataCollection  *mongo.Collection
	googleStorageService   services.GoogleStorageService
	elasticsearchService   services.ElasticsearchService
}

func NewUploadDataTask(mongoCLient *mongo.Client, googleStorageService services.GoogleStorageService,
	elasticsearchService services.ElasticsearchService) UploadDataTask {
	mongoDatabase := mongoCLient.Database(viper.GetString("mongo.database"))
	medicalModelCollection := mongoDatabase.Collection(types.MEDICAL_MODEL_COLLECTION)
	medicalDataCollection := mongoDatabase.Collection(types.MEDICAL_DATA_COLLECTION)
	return uploadDataTask{medicalModelCollection, medicalDataCollection, googleStorageService, elasticsearchService}
}

func (u uploadDataTask) fetchModelStructure(modelName string) (*types.MedicalModel, error) {

	ctx := context.Background()
	var model = types.MedicalModel{}

	filter := bson.M{"modelName": modelName}

	err := u.medicalModelCollection.FindOne(ctx, filter).Decode(&model)
	if err != nil {
		return nil, err
	}

	return &model, nil

}

func (u uploadDataTask) fetchAndWriteData(filterDataParam types.FilterDataParam, modelStructure *types.MedicalModel) {
	ctx := context.TODO()
	startRange := bson.M{"timestamp": bson.M{"$gte": primitive.NewDateTimeFromTime(time.Unix(filterDataParam.StartTime, 0))}}
	stopRange := bson.M{"timestamp": bson.M{"$lte": primitive.NewDateTimeFromTime(time.Unix(filterDataParam.EndTime, 0))}}
	modelNameFilter := bson.M{"modelName": filterDataParam.ModelName}
	deviceIdFilter := bson.M{"deviceId": filterDataParam.DeviceId}
	filter := bson.M{"$and": bson.A{startRange, stopRange, modelNameFilter, deviceIdFilter}}
	cur, err := u.medicalDataCollection.Find(ctx, filter)

	if err == mongo.ErrNoDocuments {
		fmt.Println(err.Error())
		return
	}

	var medicalDataDocument []bson.M
	if err := cur.All(context.TODO(), &medicalDataDocument); err != nil {
		// elasaticsearch
		fmt.Println(err.Error())
		return
	}

	filename := time.Unix(filterDataParam.EndTime, 0).Format("2006_01_02_15_04_05") + ".csv"
	fmt.Println(filterDataParam.EndTime)
	file, err := os.Create(filename)
	csvWriter := csv.NewWriter(file)

	allKeys := make([]string, 0)
	allKeys = append(allKeys, "timestamp", "deviceId")
	allKeys = append(allKeys, modelStructure.Keys...)
	csvWriter.Write(allKeys)
	defer csvWriter.Flush()

	allData := make([][]string, 0)
	for _, medicalData := range medicalDataDocument {
		data := make([]string, 0)
		for _, key := range allKeys {
			if key == "timestamp" {
				if _, ok := medicalData[key]; ok {
					i, err := strconv.ParseInt(fmt.Sprintf("%v", medicalData[key]), 10, 64)
					if err != nil {
						panic(err)
					}
					tm := time.Unix(i/1000, 0)
					data = append(data, fmt.Sprintf("%v", tm.UTC()))
				} else {
					data = append(data, fmt.Sprintf("%v", time.Now().UTC()))
				}
			} else {
				data = append(data, fmt.Sprintf("%v", medicalData[key]))
			}

		}
		allData = append(allData, data)
	}
	err = csvWriter.WriteAll(allData)

	uploadDataParam := types.UploadDataParam{ModelName: filterDataParam.ModelName, DeviceId: filterDataParam.DeviceId, Filename: filename, BucketName: "test-upload-sensor-data"}
	err = u.googleStorageService.UploadCsv(uploadDataParam)

	if err != nil {
		fmt.Println(err.Error())
	} else {
		uploadMedicalDataLog := types.UploadMedicalDataLog{
			ModelName:   filterDataParam.ModelName,
			DeviceId:    filterDataParam.DeviceId,
			StartTime:   filterDataParam.StartTime,
			EndTime:     filterDataParam.EndTime,
			CreatedDate: time.Now().UTC().Unix(),
			IsCompleted: true,
		}
		u.elasticsearchService.UploadLog("test-upload", uploadMedicalDataLog)
	}

}

func (u uploadDataTask) Upload() {
	waitChan := make(chan struct{}, MAX_CONCURRENT_JOBS)
	medicalModels, err := u.medicalModelCollection.Distinct(context.TODO(), "modelName", bson.M{}, nil)

	if err != nil {
		fmt.Println(err.Error())
	}

	for _, medicalModel := range medicalModels {
		waitChan <- struct{}{}
		ids, err := u.medicalDataCollection.Distinct(context.TODO(), "deviceId", bson.M{"modelName": medicalModel}, nil)
		modelStructure, err := u.fetchModelStructure(medicalModel.(string))
		if err != nil {
			fmt.Println(err.Error())
		}

		uploadDataSubTaskParam := types.UploadDataSubTask{
			MedicalModelName: medicalModel.(string),
			Ids:              ids,
			ModelStructure:   *modelStructure,
		}

		go func() {
			u.uploadDataSubTask(uploadDataSubTaskParam)
			<-waitChan
		}()
	}

}

func (u uploadDataTask) uploadDataSubTask(uploadDataSubTask types.UploadDataSubTask) {
	fetchEvery := viper.GetInt64("app.fetchTime")
	for _, id := range uploadDataSubTask.Ids {

		if id == nil {
			continue
		}
		// filter data from elasticsearch
		query := fmt.Sprintf(`{
  		"query": {
    		"bool": {
      			"must": [
        			{"match": {"modelName": "%v"}},
        			{"match": {"deviceId": "%v"}}
      			]
    			}
  			},
  			"sort": [
    			{
     	 			"endTime": {
       	 			"order": "desc"
      			}
    		}
  				], 
  			"size": 1 }`, uploadDataSubTask.MedicalModelName, id)
		searchResult, _ := u.elasticsearchService.Search("test-upload", query)
		filterParam := types.FilterDataParam{
			ModelName: uploadDataSubTask.MedicalModelName,
			DeviceId:  id.(string),
		}

		if searchResult == nil {
			filterParam.StartTime = 0
			filterParam.EndTime = time.Now().UTC().Unix()
		} else {
			filterParam.StartTime = int64(searchResult["endTime"].(float64))
			filterParam.EndTime = int64(searchResult["endTime"].(float64)) + int64(fetchEvery/int64(time.Minute)*60*60)
		}

		u.fetchAndWriteData(filterParam, &uploadDataSubTask.ModelStructure)
	}
}
