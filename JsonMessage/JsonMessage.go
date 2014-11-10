package JsonMessage

import (
	"strconv"
)

type MongoExport struct {
	Address    string `json:"address"`    // Mail Address
	Collection string `json:"collection"` // DB.Collection
}

func IntToString(input_num int64) string {
	return strconv.FormatInt(input_num, 10)
}
