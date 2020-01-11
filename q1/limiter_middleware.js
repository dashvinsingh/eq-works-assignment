module.exports = function(limiter) {
    return function(req, res, next) {
        const route = req.originalUrl;
        const ip = req.ip.split(":").pop()
        //Successful request
        if (limiter.makeRequest(route, ip) == true) { 
            next();
        } else {
            const retryTimeInSecs = limiter.getLimit()/1000;
            res.status(429).header("Retry-After", retryTimeInSecs).send({error: `Request limit exceeded, please try again in ${retryTimeInSecs} seconds`})
        };
    }
}