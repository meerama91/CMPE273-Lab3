package main

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"strconv"
)

type Resource struct {
	Key   int    `json:"key"`
	Value string `json:"value"`
}

var Msg int
var M map[int]string

func updater(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	log.Println("going into put")
	key := p.ByName("key_id")
	val := p.ByName("value")
	log.Println("key is", key)
	log.Println("value is", val)
	if len(M) == 0 {
		M = make(map[int]string)

	}
	i, err := strconv.Atoi(key)
	if err != nil {
		// handle error
		fmt.Println(err)

	}
	M[i] = val
	log.Println("inserted into map", M[i])
	Msg = 200
	responseJson, _ := json.Marshal(Msg)
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(201)

	fmt.Fprintf(rw, "%s", responseJson)
}
func getter(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	log.Println("going into get")
	key := p.ByName("key_id")
	i, err := strconv.Atoi(key)
	if err != nil {
		// handle error
		fmt.Println(err)

	}
	response := Resource{}
	response.Key = i
	response.Value = M[i]
	responseJson, _ := json.Marshal(response)
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(201)

	fmt.Fprintf(rw, "%s", responseJson)

}

func getterAll(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {

	response := make([]Resource, len(M))
	i := 0
	for k, v := range M {
		response[i].Key = k
		response[i].Value = v
		i = i + 1
	}
	responseJson, _ := json.Marshal(response)
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(201)

	fmt.Fprintf(rw, "%s", responseJson)
}

func main() {

	mux := httprouter.New()

	mux.GET("/keys/:key_id", getter)
	mux.GET("/keys", getterAll)
	mux.PUT("/keys/:key_id/:value", updater)
	server := http.Server{
		Addr:    "0.0.0.0:3000",
		Handler: mux,
	}
	server.ListenAndServe()
}
