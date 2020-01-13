### Dashvin Singh Submission

#### Question 1
Source code for question 1 can be found in the q1/ folder in this repository. It is currently deployed onto a heroku endpoint at: http://q1-eqworks.herokuapp.com/.

There are two parameters associated to rate limiting in this implementation: max number of requests and the time range in where the max number of requests is allowed. (i.e. x number of requests per y milliseconds).

By default x = 5 and y = 60000 (60 seconds) means that 5 requests are allowed to each endpoint in every 60 seconds.

This implementation does rate limiting for each endpoint, which was extracted from req.originalUrl. If the number of allowed requests have been exhausted for a particular request, the user is blocked from that request for 60 seconds (or how much ever we set the time parameter). 

An alternate implementation, if there are a lot of requests coming is when the number of allowed requests per time unit has been exhausted, we add the request to a priority queue with low priority. We handle requests with higher priority before allowing this lower priority request to go through. I did not implement this solution but mentioned it here as something that could potentially be worked on/scaled if need be.

#### Question 2b
I have implemented the backend portion of the application. You can find it in the q2b/ folder. This was my first ever exposure to Golang and its synchronization primitives. I have tried my best to understand the problem and write out the solution in Golang. If there are any components that are unclear, I would be more than happy to clarify my thought process.

#### General
Note: Both questions involved rate limiting. The implementation I applied involve using a dictionary/hash map where the key was the ip-address where the request originated. The ip address was extracted from the request object.

I have placed comments where I found would be helpful to understand my logic. Again, if clarification is required, I would be more than happy to discuss it. 

#### Resources
Middlewares: https://expressjs.com/en/guide/writing-middleware.html
Golang: https://golangvedu.wordpress.com/2017/02/01/a-one-pager-tutorial-of-go-part-1/
