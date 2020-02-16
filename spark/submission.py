from pyspark import SparkConf, SparkContext
import math

conf = SparkConf().setAppName("EQ Works Data Submission")
sc = SparkContext(conf=conf)

data = sc.textFile("/tmp/data/DataSample.csv")
data_poi = sc.textFile("/tmp/data/POIList.csv")

###Remove header and convert each row to a list of "columns"
#DataSample
first = data.first()
split = data.filter(lambda x: x != first)\
            .map(lambda x: x.split(","))

#POI
firstPOI = data_poi.first()
splitPOI = data_poi.filter(lambda x: x!= firstPOI)\
                    .map(lambda x: x.replace(" ", ""))\
                    .map(lambda x: x.split(","))

#Lambda Functions
add = lambda x,y: x+y

#Define constants for each index

####### Data Format
#(0) _ID='4516516',  
#(1) TimeSt='2017-06-21 00:00:00.143', 
#(2) Country='CA', 
#(3) Province='ON', 
#(4) City='Waterloo', 
#(5) Latitude='43.49347', 
#(6) Longitude='-80.49123'
######
ID, TIMEST, COUNTRY, PROVICE, CITY, LAT, LONG = 0,1,2,3,4,5,6
POI = 7 #this is used after matching POI in part 2

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

duplicateGeoAndTime =  split.map(lambda x: ((x[TIMEST], x[LAT], x[LONG]),1))\
                    .reduceByKey(add)\
                    .filter(lambda x: x[1] > 1)\
                    .map(lambda x: x[0]).collect()

cleaned = split.filter(lambda x: (x[TIMEST], x[LAT], x[LONG]) not in duplicateGeoAndTime)

##Part 2 Labeling
def distance(poi, currentLocation, squared=True):
    #Input is a tuple (latitude, longitude)
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

#Attaches minimum distance POI label to each row
with_poi = cleaned\
            .map(lambda x: \
                x + [\
                    min(\
                        [\
                        (distance((lat, long), (x[LAT], x[LONG]), squared=False), poi) for poi, lat, long, in pois\
                        ])[::-1]  #[0] add this if we don't want the distance
                    ]\
            )

print("Part 2 - sample row after finding POI with min distance")
print(with_poi.takeSample(withReplacement=True, num=1))

print("====Part 3 Start====")
#Part 3 a
# mean = sum(X_i's)/count(X_i's)
poi_distance = with_poi.map(lambda x: x[POI])
sum_distances = poi_distance.reduceByKey(add)
count = with_poi.map(lambda x:(x[POI][0], 1)).reduceByKey(add)
mean = sum_distances.join(count).map(lambda x: (x[0], x[1][0]/x[1][1]))

# sd = sqrt((sum(X_i's - mu)**2)/(count(X_i's) - 1))
sum_mean = poi_distance.join(mean)
diff_sum_mean_squared = sum_mean\
                        .map(lambda x: (x[0], (x[1][0] - x[1][1])**2))\
                        .reduceByKey(add)

sd = diff_sum_mean_squared\
                        .join(count)\
                        .map(lambda x: (x[0], (x[1][0]/(x[1][1]-1))**(1/2)))

print("Part 3a - mean and sd of POI distances")
print("Count: ", count.collect())
print("Mean: ", mean.collect())
print("SD: ", sd.collect())

#Part 3 b
print("Part 3b")
radius = with_poi
        .map(lambda x: (x[POI][0], ((x[ID], x[COUNTRY], x[PROVICE], x[CITY], x[LAT], x[LONG]), x[POI][1])))\
        .reduceByKey(lambda x, y: x if(x[1]>y[1]) else y)
print("Radius/i.e. max distance from POI: ", radius.collect())


area = radius.map(lambda x: (x[0], math.pi * (x[1][1])**2))
print("Area by POI: ", str(area.collect()))

density = area.join(count).map(lambda x: (x[0], x[1][1]/x[1][0]))
print("Density by POI: ", str(density.collect()))

##Comments
# There are some outliers with non negative longitude, i.e. a point not in north ameria, leading to large radius.
