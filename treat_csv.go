package main

import (
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	RequestTimeoutSeconds = 180
)

var flagsDef = map[string]string{
	"input":  "a name of input file",
	"output": "a name of output file",
	"sub":    "a type of files that you will treat",
}

var flags = make(map[string]*string)
var timeout = time.Duration(time.Duration(RequestTimeoutSeconds) * time.Second)
var client = &http.Client{Timeout: timeout}
var mutex sync.Mutex

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
	case "treat-process":
		treatMissingParts(values)
	default:
		panic("Not mapped function")
	}

}

func treatMissingParts(values []map[string]string) {
	var url = "http://localhost:8888"

	entities := getEntityValues(url, values)
	noDocList := getEntities(entities, false)
	docList := getEntities(entities, true)
	noDocNoRepeat := getTrullyNotCapturedProcess(docList, noDocList)

	another := getListOfProcess(len(noDocNoRepeat), noDocNoRepeat)
	fmt.Println("doc ", len(docList))
	fmt.Println("nodoc ", len(noDocList))
	fmt.Println("nodoc noRepeat", len(noDocNoRepeat))
	for _, entity := range noDocNoRepeat {
		if lastUpdated, ok := entity["_lastUpdate"].(string); ok {
			fmt.Println(entity["processo"].(string) + "," + lastUpdated + "," + entity["tribunal"].(string))
		}
	}
	fmt.Println("total: ", len(entities))
	fmt.Println("map no repeat", len(another))

}

func getTrullyNotCapturedProcess(doc, noDoc []map[string]interface{}) []map[string]interface{} {
	intersection := make([]map[string]interface{}, 0)
	for _, d := range doc {
		for _, n := range noDoc {
			if d["processo"] == n["processo"] {
				intersection = append(intersection, n)
			}
		}
	}
	noRepeat := make([]map[string]interface{}, 0)
	for _, n := range noDoc {
		equal := false
		for _, i := range intersection {
			if i["processo"] == n["processo"] {
				equal = true
			}
		}
		if !equal {
			noRepeat = append(noRepeat, n)
		}
	}
	return noRepeat

}

func getListOfProcess(size int, noDoc []map[string]interface{}) map[string]interface{} {
	m := make(map[string]interface{}, size)
	c := 0
	for i, entity := range noDoc {
		if m[entity["processo"].(string)] != nil {
			c++
		}
		m[entity["processo"].(string)] = i
	}
	fmt.Println("count", c)
	return m
}

func getEntities(entities []map[string]interface{}, withDoc bool) []map[string]interface{} {
	noDoc := make([]map[string]interface{}, 0)
	targetKey := []string{"ativos", "passivos", "outrasPartes"}
	notAggregated := false
	var countAtivos, countPassivos, countOutras int
	for _, e := range entities {
		for _, key := range targetKey {
			if part, okPart := e[key].([]interface{}); okPart {
				if isNotAgregated(part, withDoc) {
					if key == "ativos" {
						countAtivos++
					}
					if key == "passivos" {
						countPassivos++
					}
					if key == "outrasPartes" {
						countOutras++
					}
					noDoc = append(noDoc, e)
					notAggregated = true
				}
			}
			if notAggregated {
				notAggregated = false
				break
			}
		}

	}
	fmt.Printf("ativos: %v - passivos: %v - outras: %v\n", countAtivos, countPassivos, countOutras)
	return noDoc
}

func isNotAgregated(part []interface{}, withDoc bool) bool {
	for _, pInterface := range part {
		p := pInterface.(map[string]interface{})
		if name, okName := p["nome"].(string); okName {
			if strings.Contains(name, "NOME EMPRESA") {
				if withDoc {
					if _, okDoc := p["cnpj"].(string); okDoc {
						return true
					}
				} else {
					if _, okDoc := p["cnpj"].(string); !okDoc {
						return true
					}
				}
			}

		}
	}
	return false
}

func getEntityValues(url string, values []map[string]string) []map[string]interface{} {
	//define concurrency
	routines := 20
	control := make(chan bool, routines)
	chanValue := make(chan map[string]interface{}, len(values))
	var wg sync.WaitGroup

	e := make([]map[string]interface{}, 0)
	for _, input := range values {
		wg.Add(1)
		control <- true
		id := input["id"]

		go func(id string) {
			defer wg.Done()
			defer func() { <-control }()

			u := url + id
			g, err := http.NewRequest("GET", u, nil)
			checkErr(err)
			g.Header.Set("Authorization", "auth")
			fmt.Println("Get document: ", id)

			resp, err := doRequest(g)
			if err != nil {
				fmt.Println("Error getting request! ", err)
			}
			if resp.StatusCode == http.StatusOK {
				defer resp.Body.Close()
				record := make(map[string]interface{})
				bodyResponse, err := ioutil.ReadAll(resp.Body)
				checkErr(err)
				if len(bodyResponse) > 0 {
					err = json.Unmarshal(bodyResponse, &record)
					checkErr(err)
				}
				chanValue <- record
			}
		}(id)
	}
	wg.Wait()
	close(chanValue)
	m := make(map[string]interface{}, len(values))
	for i := range chanValue {
		e = append(e, i)
		id := i["_id"].(string)
		m[id] = i
	}
	return e
}

func doRequest(req *http.Request) (*http.Response, error) {
	resp, err := client.Do(req)
	return resp, err
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
