package kinesis

import (
	"fmt"

	"github.com/golang/protobuf/proto"
)

// MaxBytesPerRecord : Maximum size for a Kinesis record.
const MaxBytesPerRecord = 1024 * 1024 // 1 MB

// AggregatedRecordFull : Error for adding too many records to an AggregatedRecord
type AggregatedRecordFull struct {
	AggSize    int // Size of AggregatedRecord
	RecordSize int // Size of Record to add
}

func (e *AggregatedRecordFull) Error() string { return "AggregatedRecord Full: Unable to add record." }

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

// AddUserRecord : Add a kinesis record to an Aggregated Record
func (m *AggregatedRecord) AddUserRecord(partitionKey string, explicitHashKey string, userRecord []byte) error {
	// Determine weather or not there is room to add the record.
	postOpSize := proto.Size(m) + len(userRecord)
	if postOpSize > MaxBytesPerRecord {
		err := &AggregatedRecordFull{
			AggSize:    proto.Size(m),
			RecordSize: len(userRecord),
		}
		return err
	}

	// Error if PartitionKey is ""
	if partitionKey == "" {
		return fmt.Errorf("Empty partition key is not allowed!")
	}

	// Initialize a Kinesis Record from the given user data.
	record := &Record{Data: userRecord}

	// Search for the partition key in the AggregatedRecord and add the index to
	// the record. Add the key to the table if it isn't already there
	found, partitionIndex := findString(m.PartitionKeyTable, partitionKey)
	if !found {
		m.PartitionKeyTable[partitionIndex] = partitionKey
	}
	record.PartitionKeyIndex = &partitionIndex

	// If there is an explicitHashKey do the same as above. (explicitHashKey is optional)
	if explicitHashKey != "" {
		found, hashKeyIndex := findString(m.ExplicitHashKeyTable, explicitHashKey)
		if !found {
			m.ExplicitHashKeyTable[hashKeyIndex] = explicitHashKey
		}
		record.ExplicitHashKeyIndex = &hashKeyIndex
	}

	m.Records = append(m.Records, record)
	return nil
}

// findString : Look for a string in a slice. If it is there return (true, index),
//              Else, return (false, len(slice)). The idea being that you can use
//              The length of the slice to append the new string if you so wish.
func findString(collection []string, thing string) (bool, uint64) {
	for index, value := range collection {
		if value == thing {
			return true, uint64(index)
		}
	}
	return false, uint64(len(collection))
}
