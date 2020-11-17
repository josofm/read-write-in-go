package main

import (
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
)

var flagsDef = map[string]string{
	"input":  "a name of input file",
	"output": "a name of output file",
	"sub":    "a type of files that you will treat",
}

var flags = make(map[string]*string)

func init() {
	for k, v := range flagsDef {
		var flagTemp string
		flag.StringVar(&flagTemp, k, "", v)
		flags[k] = &flagTemp
	}
}

func main() {
	flag.Parse()
	input := *flags["input"]
	output := *flags["output"]
	targetFunc := *flags["sub"]

	if input == "" || output == "" || !strings.Contains(input, ".csv") || !strings.Contains(output, ".csv") {
		panic("Invalid name of input/output file")
	}
	values := getValuesByFile(input)
	calculateNewValues(targetFunc, values)

	createFile(output, values)

}

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}

func getValuesByFile(input string) []map[string]string {

	csvFile, err := os.Open(input)
	checkErr(err)

	var header []string
	r := csv.NewReader(csvFile)
	if header, err = r.Read(); err != nil { //read header
		log.Fatal(err)
		checkErr(err)
	}
	values := []map[string]string{}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		checkErr(err)
		line := map[string]string{}
		for i := range header {
			line[header[i]] = record[i]
		}
		values = append(values, line)

	}
	return values
}

func createFile(output string, values []map[string]string) {
	file, err := os.Create(output)
	checkErr(err)
	defer file.Close()
	writer := csv.NewWriter(file)

	var header []string
	for i := range values[0] {
		header = append(header, i)
	}
	if err := writer.Write(header); err != nil {
		checkErr(err)
	}
	for _, dict := range values {
		var value []string
		for _, h := range header {
			value = append(value, dict[h])
		}

		if err := writer.Write(value); err != nil {
			checkErr(err)
		}
	}

	writer.Flush()

}

func calculateNewValues(targetFunc string, values []map[string]string) {
	switch targetFunc {
	case "process":
		calculateBasedOnProcessFile(values)
	default:
		panic("Not mapped function")
	}

}

func calculateBasedOnProcessFile(values []map[string]string) {
	for i, v := range values {
		temp := make(map[string]interface{})
		if v["source"] == "someSource" {
			temp["field"] = v["rule1"] + v["rule2"]
		} else if strings.Contains(v["source"], "some-string") {
			temp["field"] = v["generic"] + v["generic2"]
		} else {
			temp["field"] = v["what"] + v["you"] + v["want"]
		}
		values[i]["sourceID"] = createId(temp)
	}
}

func createId(input map[string]interface{}) string {
	h := md5.New()

	keys := make([]string, len(input))
	i := 0
	for key, _ := range input {
		keys[i] = key
		i++
	}
	sort.Strings(keys)

	for _, value := range keys {
		var id string
		if reflect.TypeOf(input[value]).String() == "map[string]interface {}" {
			id = fmt.Sprint(createId(input[value].(map[string]interface{})))
		} else {
			id = fmt.Sprint(input[value])
		}
		io.WriteString(h, id)
	}
	return hex.EncodeToString(h.Sum(nil))
}
