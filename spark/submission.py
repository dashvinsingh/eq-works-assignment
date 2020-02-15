from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("EQ Works Data Submission")
sc = SparkContext(conf=conf)

data = sc.textFile("/tmp/data/DataSample.csv")
data_poi = sc.textFile("/tmp/data/POIList.csv")

#DataSample
first = data.first()
split = data.filter(lambda x: x != first).map(lambda x: x.split(","))

#POI
firstPOI = data_poi.first()
splitPOI = data_poi.filter(lambda x: x!= firstPOI)\
                    .map(lambda x: x.replace(" ", ""))\
                    .map(lambda x: x.split(","))

#Lambda Functions
add = lambda x,y: x+y

##Part 1 Cleanup

####We don't want to filter out (time) and (geo info) but rather (time and geo info)
# duplicateTimeStamps =  split.map(lambda x: (x[1],1))\
#                             .reduceByKey(lambda x,y: x+y)\
#                             .filter(lambda x: x[1] > 1)\
#                             .map(lambda x: x[0]).collect()

# duplicateGeoInfo =  split.map(lambda x: ((x[5], x[6]),1))\
#                     .reduceByKey(lambda x,y: x+y)\
#                     .filter(lambda x: x[1] > 1)\
#                     .map(lambda x: x[0]).collect()

duplicateGeoAndTime =  split.map(lambda x: ((x[1], x[5], x[6]),1))\
                    .reduceByKey(add)\
                    .filter(lambda x: x[1] > 1)\
                    .map(lambda x: x[0]).collect()

cleaned = split.filter(lambda x: (x[1], x[5], x[6]) not in duplicateGeoAndTime)

##Part 2 Labeling
def distance(poi, currentLocation, squared=True):
    #Input is a tuple (latitude, longitude)
    import math
    poi_lat, poi_long = float(poi[0]), float(poi[1])
    current_lat, current_long = float(currentLocation[0]), float(currentLocation[1])
    if (squared == True):
        #returns squared distance
        return ((current_lat - poi_lat)**2) + ((current_long - poi_long)**2)
    else:
        #returns haversine distance
        # Source: https://www.movable-type.co.uk/scripts/latlong.html
        lat1_rad = math.radians(current_lat)
        lat2_rad = math.radians(poi_lat)

        d_lat = math.radians(poi_lat - current_lat)
        d_long = math.radians(poi_long - current_long)

        a = math.sin(d_lat/2)**2 + math.cos(lat1_rad)*math.cos(lat2_rad)*(math.sin(d_long/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

        #6371 is earth's average radius in Meters
        return 6371 * c

#Adds a new tuple to each row (POI#, distance)
pois = splitPOI.collect()
with_poi = cleaned\
            .map(lambda x: \
                x + [\
                    min(\
                        [\
                        (distance((lat, long), (x[5], x[6]), squared=False), poi) for poi, lat, long, in pois\
                        ])[::-1]  #[0] add this if we don't want the distance
                    ]\
            )

print("Part 2 - sample row after finding POI with min distance")
print(with_poi.takeSample(withReplacement=True, num=1))
#Part 3 a
# mean = sum(X_i's)/count(X_i's)
poi_distance = with_poi.map(lambda x:x[7])
sum_distances = poi_distance.reduceByKey(add)
count = with_poi.map(lambda x:(x[7][0], 1)).reduceByKey(add)
mean = sum_distances.join(count).map(lambda x: (x[0], x[1][0]/x[1][1]))

sum_mean = poi_distance.join(mean)
diff_sum_mean_squared = sum_mean.map(lambda x: (x[0], (x[1][0] - x[1][1])**2)).reduceByKey(add)
sd = diff_sum_mean_squared.join(count).map(lambda x: (x[0], (x[1][0]/(x[1][1]-1))**(1/2)))

print("Part 3a - mean and sd of POI distances")
print("Mean: ", mean.collect())
print("SD: ", sd.collect())

#Part 3 b


####### Data Format
#(0) _ID='4516516',  
#(1) TimeSt='2017-06-21 00:00:00.143', 
#(2) Country='CA', 
#(3) Province='ON', 
#(4) City='Waterloo', 
#(5) Latitude='43.49347', 
#(6) Longitude='-80.49123'
######