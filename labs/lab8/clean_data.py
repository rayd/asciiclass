import csv
import math
import json

def calculate_min_distance(pickup_loc):
    min_dist = 999999
    min_dist_location = None
    for interest_pt in interest_points:
        dist = math.sqrt(math.pow(float(pickup_loc["latitude"]) - float(interest_pt["lat"]), 2) + math.pow(float(pickup_loc["longitude"]) - float(interest_pt["lon"]), 2))
        # print "calculating sqrt( (",float(pickup_loc["latitude"]),"-",float(interest_pt["lat"]),")^2 + (",float(pickup_loc["longitude"]),"-",float(interest_pt["lon"]),")^2 )"
        # print "distance to",interest_pt["name"],"is",dist
        if(dist < min_dist):
            min_dist = dist
            min_dist_location = interest_pt
    # print "------"
    return min_dist_location

interest_points = []
interest_points_reader = csv.DictReader(open("interestpoints.csv"))
for row in interest_points_reader:
    interest_points.append({ "name": row["NAME"], "lat": row["LAT"], "lon": row["LONG"], "pickups": [] })

count = 0
taxi_data = csv.DictReader(open("traindata.csv"), fieldnames=["trip_id","time","address","longitude","latitude","dropoff_id"])
for row in taxi_data:
    interest_pt = calculate_min_distance(row)
    interest_pt["pickups"].append(row["time"])
    count += 1
    if((count % 50000) == 0):
        print (count / 4186461.0)*100,"%"
json.dump(interest_points, open("pickups.json", "w"))

