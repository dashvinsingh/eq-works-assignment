package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type counters struct {
	sync.Mutex
	view  int
	click int
}

var (
	m = make(map[string]*counters)
	content = []string{"sports", "entertainment", "business", "education"}
)

func welcomeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Welcome to EQ Works 😎")
}

func viewHandler(w http.ResponseWriter, r *http.Request) {
	data := content[rand.Intn(len(content))]
	
	//Create key of format "content string" : "date and time"
	now := time.Now()
	newKey := data + ":" + now.Format("2006-01-02 15:04")
	
	//If key does not exist in map, initiate new key
	_, ok := m[newKey]; if ! ok {
		m[newKey] = &counters{}
	}

	//Access counters struct belonging to newKey and increment view count synchronously.
	c := m[newKey]
	c.Lock()
	c.view++
	c.Unlock()

	err := processRequest(r)
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(400)
		return
	}


	// simulate random click call
	if rand.Intn(100) < 50 {
		processClick(newKey)
	}
}

func processRequest(r *http.Request) error {
	time.Sleep(time.Duration(rand.Int31n(50)) * time.Millisecond)
	return nil
}

func processClick(data string) error {
	c := m[data]
	c.Lock()
	c.click++
	c.Unlock()

	return nil
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	if !isAllowed() {
		w.WriteHeader(429)
		return
	}
}

func isAllowed() bool {
	return true
}

func uploadCounters() error {
	return nil
}

func main() {
	http.HandleFunc("/", welcomeHandler)
	http.HandleFunc("/view/", viewHandler)
	http.HandleFunc("/stats/", statsHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
