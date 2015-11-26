package main

import (
	"errors"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"hash/crc32"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Resource struct {
	Key   int    `json:"key"`
	Value string `json:"value"`
}

var c *Consistent

type uints []uint32

// Len returns the length of the uints array.
func (x uints) Len() int { return len(x) }

// Less returns true if element i is less than element j.
func (x uints) Less(i, j int) bool { return x[i] < x[j] }

// Swap exchanges elements i and j.
func (x uints) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

// ErrEmptyCircle is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyCircle = errors.New("empty circle")

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	circle           map[uint32]string
	members          map[string]bool
	sortedHashes     uints
	NumberOfReplicas int
	count            int64
	scratch          [64]byte
	sync.RWMutex
}

// eltKey generates a string key for an element with an index.
func (c *Consistent) eltKey(elt string, idx int) string {
	// return elt + "|" + strconv.Itoa(idx)
	return strconv.Itoa(idx) + elt
}

// Add inserts a string element in the consistent hash.
func (c *Consistent) Add(elt string) {
	c.Lock()
	defer c.Unlock()
	c.add(elt)
}


func (c *Consistent) add(elt string) {
	for i := 0; i < c.NumberOfReplicas; i++ {
		c.circle[c.hashKey(c.eltKey(elt, i))] = elt
	}
	c.members[elt] = true
	c.updateSortedHashes()
	c.count++
}


func (c *Consistent) remove(elt string) {
	for i := 0; i < c.NumberOfReplicas; i++ {
		delete(c.circle, c.hashKey(c.eltKey(elt, i)))
	}
	delete(c.members, elt)
	c.updateSortedHashes()
	c.count--
}

// Set sets all the elements in the hash.  If there are existing elements not
// present in elts, they will be removed.
func (c *Consistent) Set(elts []string) {
	c.Lock()
	defer c.Unlock()
	for k := range c.members {
		found := false
		for _, v := range elts {
			if k == v {
				found = true
				break
			}
		}
		if !found {
			c.remove(k)
		}
	}
	for _, v := range elts {
		_, exists := c.members[v]
		if exists {
			continue
		}
		c.add(v)
	}
}

func (c *Consistent) Members() []string {
	c.RLock()
	defer c.RUnlock()
	var m []string
	for k := range c.members {
		m = append(m, k)
	}
	return m
}

// Get returns an element close to where name hashes to in the circle.
func (c *Consistent) Get(name string) (string, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return "", ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	return c.circle[c.sortedHashes[i]], nil
}

func (c *Consistent) search(key uint32) (i int) {
	f := func(x int) bool {
		return c.sortedHashes[x] > key
	}
	i = sort.Search(len(c.sortedHashes), f)
	if i >= len(c.sortedHashes) {
		i = 0
	}
	return
}

func (c *Consistent) hashKey(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) updateSortedHashes() {
	hashes := c.sortedHashes[:0]
	
	if cap(c.sortedHashes)/(c.NumberOfReplicas*4) > len(c.circle) {
		hashes = nil
	}
	for k := range c.circle {
		hashes = append(hashes, k)
	}
	sort.Sort(hashes)
	c.sortedHashes = hashes
}

func sliceContainsMember(set []string, member string) bool {
	for _, m := range set {
		if m == member {
			return true
		}
	}
	return false
}

func updater(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	log.Println("going into put")
	//c := New()
	key := p.ByName("key_id")
	value := p.ByName("value")
	log.Println("key is", key)
	log.Println("value is", value)
	var url string
	var response int
	server, err1 := c.Get(key)
	if err1 != nil {
		log.Fatal(err1)
	}
	log.Println("server is", server)
	if strings.Compare(server, "cache0") == 0 {
		url = "http://localhost:3000/keys/" + key + "/" + value

		//    req.Header.Set("Content-Type", "application/json")

	} else if strings.Compare(server, "cache1") == 0 {
		url = "http://localhost:3001/keys/" + key + "/" + value

	} else if strings.Compare(server, "cache2") == 0 {

		url = "http://localhost:3002/keys/" + key + "/" + value
	}
	log.Println("url is", url)
	req1, err := http.NewRequest("PUT", url, strings.NewReader(""))
	res := &http.Client{}
	resp, err := res.Do(req1)
	if err != nil {
		panic(err)
	}
	body, _ := ioutil.ReadAll(resp.Body)
	//	json.Unmarshal(body, response)
	log.Println("response from server is", response)
	//responseJson, _ := json.Marshal(response)
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(201)
	fmt.Fprintf(rw, "%s", body)

}

func getter(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {

	key := p.ByName("key_id")
	var url string
	
	server, err := c.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	if strings.Compare(server, "cache0") == 0 {
		url = "http://localhost:3000/keys/" + key
	} else if strings.Compare(server, "cache1") == 0 {
		url = "http://localhost:3001/keys/" + key
	} else if strings.Compare(server, "cache2") == 0 {
		url = "http://localhost:3002/keys/" + key
	}
	log.Println("url is", url)
	resp, err := http.Get(url)
	body, _ := ioutil.ReadAll(resp.Body)
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(201)
	fmt.Fprintf(rw, "%s", body)

}

func main() {

	c = new(Consistent)
	c.NumberOfReplicas = 20
	c.circle = make(map[uint32]string)
	c.members = make(map[string]bool)
	c.Add("cache0")
	c.Add("cache1")
	c.Add("cache2")
	mux := httprouter.New()
	mux.GET("/keys/:key_id", getter)
	mux.PUT("/keys/:key_id/:value", updater)
	client := http.Server{
		Addr:    "0.0.0.0:8080",
		Handler: mux,
	}
	client.ListenAndServe()

}
