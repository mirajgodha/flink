# Analyze the Cab data

Data is of the following schema
```csv
cab id, cab number plate, cab type, cab driver name, ongoing trip/not, pickup location, destination,passenger count
```
Using Datastream/Dataset transformations find the following for each ongoing trip.

- Popular destination.  | Where more number of people reach.
- Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.
- Average number of trips for each driver.  | average =  total no. of passengers drivers has picked / total no. of trips he made

