package mapreduce

import (
	"encoding/json"
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

	//fmt.Println(jobName,reduceTask,outFile,nMap)
	//fmt.Println("RIP")
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
	var omfiles []string
	for i:=0;i<nMap;i++{
		omfiles= append(omfiles, reduceName(jobName,i,reduceTask))
	}
	var kvlist map[string]([]string)
	kvlist=make(map[string]([]string))
	var keylist ([]string)
	for i:=0;i<nMap;i++{
		omfile:=omfiles[i]
		fptr,_:=os.Open(omfile)
		decr:=json.NewDecoder(fptr)
		var kv KeyValue
		err:=decr.Decode(&kv)
		for err==nil{
			tempkey:=kv.Key
			tempval:=kv.Value
			exist:=kvlist[tempkey]
			if exist==nil {
				//tempint,_:=strconv.Atoi(tempkey)
				keylist= append(keylist, tempkey)
				kvlist[tempkey]=nil
			}
			kvlist[tempkey]=append(kvlist[tempkey],tempval)
			err=decr.Decode(&kv)
		}
		fptr.Close()
	}

	sort.Strings(keylist)

	var ofptr *os.File
	ofptr,_=os.Create(outFile)
	encr:=json.NewEncoder(ofptr)
	for i:=0;i< len(keylist);i++ {
		tempkey:=keylist[i]
		var tempStr string
		tempStr=reduceF(tempkey,kvlist[tempkey])
		encr.Encode(KeyValue{tempkey,tempStr})
	}
	ofptr.Close()

}
