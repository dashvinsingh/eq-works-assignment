class RateLimiter {
    constructor(maxRequest, timeLimit) {
        this.maxRequest = maxRequest;
        this.timeLimit = timeLimit;
        this.memory = {};
    }

    makeRequest(route, ipAddress) {

        //Validation and creation of initial objects
        if (!(route in this.memory)) {
            this.memory[route] = {};
        }
        if (!(ipAddress in this.memory[route])) {
            this.memory[route][ipAddress] = {requestCount: 0, requestTime: Date.now(), active: true}
        }

        //Get current time and count.
        const startTime = this.getStartTime(route, ipAddress);
        const currentCount = this.getCurrentCount(route, ipAddress);
       
        //If the time now is in the range of [startTime, startTime + timeLimit) and currentCount does not exeed limits
        //carry on.
        if ((startTime <= Date.now() && Date.now() < startTime + this.timeLimit) && currentCount < this.maxRequest) {
            this.memory[route][ipAddress].active = true;
            this.memory[route][ipAddress].requestCount++;
            return true
        } else {
            //On first execution of the else statement, reset the counter and set startTime to be old start time + timeLimit.
            if (this.memory[route][ipAddress].active == true) {
                // this.setStartTime(route, ipAddress, startTime + this.timeLimit) //resets to timeLimit after initial startTime
                this.setStartTime(route, ipAddress, Date.now() + this.timeLimit) //resets to timeLimit after the last request
                this.memory[route][ipAddress].active = false;
                this.memory[route][ipAddress].requestCount = 0;
            }
            return false
        }
        
    }
    
    //Setters and Getters to minimize direct memory/object lookups.
    getCurrentCount(route, ipAddress) {
        return this.memory[route][ipAddress].requestCount;
    }
    getStartTime(route, ipAddress) {
        return this.memory[route][ipAddress].requestTime;
    }
    setStartTime(route, ipAddress, startTime) {
        this.memory[route][ipAddress].requestTime = startTime;
    }
    getMemory() {
        return this.memory;
    }
    getIPData(route, ipAddress) {
        return this.memory[route][ipAddress];
    }
    getLimit() {
        return this.timeLimit;
    }

}

module.exports = {RateLimiter: RateLimiter}