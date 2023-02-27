package types

type UploadMedicalDataLog struct {
	ModelName   string `json:"modelName"`
	DeviceId    string `json:"deviceId"`
	StartTime   int64  `json:"startTime"`
	EndTime     int64  `json:"endTime"`
	CreatedDate int64  `json:"createdDate"`
	IsCompleted bool   `json:"isCompleted"`
}
