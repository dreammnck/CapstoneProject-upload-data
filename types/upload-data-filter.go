package types

type FilterDataParam struct {
	ModelName string
	DeviceId  string
	StartTime int64
	EndTime   int64
}

type UploadDataParam struct {
	ModelName  string 
	DeviceId   string
	BucketName string
	Filename   string
}


type MedicalModel struct {
	ID          string   `json:"id" bson:"_id,omitempty"`
	ModelName   string   `json:"modelName" bson:"modelName"`
	Keys        []string `yaml:"keys" json:"keys" bson:"keys"`
	DataTypes   []string `yaml:"datatypes" json:"dataTypes" bson:"dataTypes"`
	FieldLength int      `yaml:"fieldLength" json:"fieldLength" bson:"fieldLength"`
	Tags        []string `json:"tags" bson:"tags"`
}

type UploadDataSubTask struct {
	MedicalModelName   string
	Ids            []interface{}
	ModelStructure MedicalModel
}