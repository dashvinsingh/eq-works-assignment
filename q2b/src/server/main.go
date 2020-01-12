package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
	"strings"
)

type counters struct {
	sync.Mutex
	view  int
	click int
}

//This struct contains number of requests, time of first request, 
//and whether ip is active.
type rateLimitData struct {
	requestCount int
	requestTime time.Time
	active bool
}

var (

	//Rate Limit (statHandler) Variables
	rateLimiterMap = make(map[string]*rateLimitData)
	maxRequest = 5 			//5 requests allowed every 1 min
	timeLimit time.Duration = 1000 * 60; 	//1 minute in ms

	//viewHandler Variables
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
	ipAddress := strings.Split(r.RemoteAddr,":")[0]
	//isAllowed modified to handle rate limiting logic
	if !isAllowed(ipAddress) {
		w.WriteHeader(429)
		return
	}
}

func isAllowed(ipAddress string) bool {
	//If ipAddress not already in the hash map, initiate it with
	//ipAddress as the key and rateLimitData struct as its value
	_, ok := rateLimiterMap[ipAddress]; if ! ok {
		rateLimiterMap[ipAddress] = &rateLimitData{0, time.Now(), true}
	}

	startTime := rateLimiterMap[ipAddress].requestTime
	currentCount := rateLimiterMap[ipAddress].requestCount

	//If time now is in range of [startTime, startTime + timeLimit] and we haven't exhaused request limit
	if (time.Now().After(startTime) && time.Now().Before(startTime.Add(time.Millisecond * timeLimit))) && currentCount < maxRequest {
		rateLimiterMap[ipAddress].active = true
		rateLimiterMap[ipAddress].requestCount++
		return true
	} else {
		//If request limit exhaused, mark ip as inactive and set new time (i.e. time when this user can resume).
		if (rateLimiterMap[ipAddress].active == true) {
			rateLimiterMap[ipAddress].requestTime = time.Now().Add(time.Millisecond * timeLimit) 
			rateLimiterMap[ipAddress].active = false
			rateLimiterMap[ipAddress].requestCount = 0
		}
	}
	return false
}

func uploadCounters() error {
	for true {
		processRequest(nil)
		// fmt.Println("Processing Request.")
		time.Sleep(time.Second * 5)
	}
	return nil
}

func main() {
	http.HandleFunc("/", welcomeHandler)
	http.HandleFunc("/view/", viewHandler)
	http.HandleFunc("/stats/", statsHandler)

	//Creates a new thread uploading data (i.e. processing request) every 5 seconds.
	go uploadCounters()

	//Printing parameters of rate limiter.
	fmt.Printf("Rate Limiter Config\nMax Requests: %d\nTime Limit: %s", maxRequest, timeLimit)
	
	log.Fatal(http.ListenAndServe(":8080", nil))
}
