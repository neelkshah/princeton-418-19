package mapreduce

import (
	"encoding/json"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.
	var data = make(map[string][]string)
	for index := 0; index < nMap; index++ {
		fileName := reduceName(jobName, index, reduceTaskNumber)
		f, err := os.OpenFile(fileName, os.O_RDONLY, 0660)
		checkError(err)
		decoder := json.NewDecoder(f)
		for decoder.More() {
			var pair KeyValue
			decoder.Decode(&pair)
			if _, exists := data[pair.Key]; !exists {
				data[pair.Key] = make([]string, 0)
			}
			data[pair.Key] = append(data[pair.Key], pair.Value)
		}
		f.Close()
	}
	var outputFile = mergeName(jobName, reduceTaskNumber)
	handler, err := os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE, 0660)
	checkError(err)
	enc := json.NewEncoder(handler)
	for key, values := range data {
		enc.Encode(KeyValue{key, reduceF(key, values)})
	}
	handler.Close()
}
