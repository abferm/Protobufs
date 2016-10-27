package kinesis

import (
	"fmt"

	"github.com/golang/protobuf/proto"
)

// RecordFromProtobuf : Create a Kinesis Record with the given protobuf message as a payload
func RecordFromProtobuf(partitionKeyIndex *uint64, hashKeyIndex *uint64, payload proto.Message, tags ...*Tag) (*Record, error) {
	var record = &Record{}
	data, err := proto.Marshal(payload)
	if err != nil {
		return record, fmt.Errorf("Error marshaling payload.\t", err)
	}
	record.PartitionKeyIndex = partitionKeyIndex
	record.ExplicitHashKeyIndex = hashKeyIndex
	record.Data = data
	return record, nil
}

// NewAggregatedRecord : Turn a list of Kinesis Records into an AggregatedRecord
func NewAggregatedRecord(partitionKeyTable []string, hashKeyTable []string, records []*Record) *AggregatedRecord {
	ar := &AggregatedRecord{}
	ar.PartitionKeyTable = partitionKeyTable
	ar.ExplicitHashKeyTable = hashKeyTable
	ar.Records = records
	return ar
}
