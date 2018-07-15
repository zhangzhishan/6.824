package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	// fmt.Println("do reducing")
	var beforReduce []KeyValue
	kvmaps := make(map[string][]string)
	// var keys []string
	// var values []string
	for m := 0; m < nMap; m++ {
		filename := reduceName(jobName, m, reduceTask)
		fmt.Println(filename)
		file, _ := os.Open(filename)

		dec := json.NewDecoder(file)

		for {
			var temp KeyValue
			if err := dec.Decode(&temp); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			// fmt.Printf("%s: %s\n", m.Key, m.Value)
			// keys = append(keys, temp.Key)
			// values = append(values, temp.Value)
			beforReduce = append(beforReduce, temp)
		}

	}
	// fmt.Println("1 reducing")

	sort.Slice(beforReduce, func(i, j int) bool {
		return beforReduce[i].Key < beforReduce[j].Key
	})
	// sort.Strings(keys)
	// fmt.Println("2 reducing")
	for _, kv := range beforReduce {
		kvmaps[kv.Key] = append(kvmaps[kv.Key], kv.Value)
	}

	outfile, _ := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE, 0755)
	fmt.Println(outFile)
	enc := json.NewEncoder(outfile)
	// fmt.Println("3 reducing")

	for key := range kvmaps {
		enc.Encode(KeyValue{key, reduceF(key, kvmaps[key])})
	}
	// fmt.Println("finish reducing")

}
