
# **Practice Exercises**

## **Package: `dataStream` - Basic DataStream API**

### Exercise 1: Real-Time Click Stream Analytics

**Objective:** Learn basic DataStream transformations (map, filter, keyBy)

**Task:**
Create a Flink program that:

1. Reads click events from a socket (format: `timestamp,userId,productId,action`)
2. Filters only "purchase" actions
3. Counts purchases per user
4. Prints users with more than 3 purchases

***

## **Package: `windowing` - Time Windows and Aggregations**

### Exercise 2: E-Commerce Sales Dashboard

**Objective:** Master tumbling and sliding windows with event-time processing

**Task:**
Create two Flink jobs:

1. **Tumbling Window Job**: Calculate total sales per product category every 10 seconds
2. **Sliding Window Job**: Calculate moving average of order values with 30-second window sliding every 10 seconds

**Data Generator:** Emit `timestamp,category,productName,saleAmount`

**Expected Skills:** TumblingEventTimeWindows, SlidingEventTimeWindows, watermark strategies, window aggregations

***

## **Package: `sideOutputs` - Side Output Pattern**

### Exercise 3: Traffic Violation Detection System

**Objective:** Learn to split streams using OutputTags and side outputs

**Task:**
Process vehicle speed data and split into three streams:

- Main output: Normal speed vehicles (≤ 60 km/h)
- Side output 1: Minor violations (61-80 km/h)
- Side output 2: Major violations (> 80 km/h)

**Data Format:** `timestamp,vehicleId,speed,location`

**Expected Skills:** ProcessFunction, OutputTag, getSideOutput()

***

## **Package: `stateful` - Stateful Stream Processing**

### Exercise 4: User Session Tracker

**Objective:** Implement stateful processing with ValueState

**Task:**
Track user sessions and detect:

1. Session start (first event from user)
2. Session activity count
3. Session timeout (no activity for 60 seconds)
4. Emit session summary on timeout

**Data Format:** `timestamp,userId,action,page`

**Expected Skills:** KeyedProcessFunction, ValueState, timers, onTimer()

***

## **Package: `iterate` - Iterative Streams**

### Exercise 5: Convergence Detection Algorithm

**Objective:** Use iterate operator for feedback loops

**Task:**
Implement a simple iterative algorithm:

1. Start with initial values (0-100)
2. In each iteration, apply transformation: `newValue = oldValue * 0.9 + 5`
3. Continue until value converges (change < 0.1) or max 10 iterations
4. Emit convergence results

**Expected Skills:** IterativeStream, feedback loops, termination conditions

***

## **Package: `joins` - Stream Joins**

### Exercise 6: Order Fulfillment Tracker

**Objective:** Join two streams with time windows

**Task:**
Join two streams:

- **Orders stream:** `orderId,customerId,timestamp,amount`
- **Shipments stream:** `orderId,timestamp,carrier,status`

Calculate average time from order to shipment per customer.

**Expected Skills:** Stream joins, TumblingEventTimeWindows with joins, time-based correlation

***

## **Package: `connectors` - External Systems Integration**

### Exercise 7: IoT Sensor Data Pipeline

**Objective:** Read from Kafka and write to file system

**Task:**

1. Consume sensor data from Kafka topic `sensor-readings`
2. Parse JSON: `{"sensorId": "...", "temperature": ..., "timestamp": ...}`
3. Filter anomalies (temperature > 100 or < -20)
4. Write to partitioned Parquet files by sensor ID

**Expected Skills:** FlinkKafkaConsumer, JSON deserialization, FileSink with partitioning

***

## **Package: `cep` - Complex Event Processing**

### Exercise 8: Fraud Detection Pattern Matching

**Objective:** Detect complex patterns using Flink CEP

**Task:**
Detect potential fraud:
Pattern: 3 failed login attempts followed by 1 successful login within 5 minutes from same IP

**Data Format:** `timestamp,userId,ipAddress,loginStatus`

**Expected Skills:** CEP Pattern API, pattern sequences, within() time constraints

***

## **Package: `table_sql` - Table API \& SQL**

### Exercise 9: Real-Time Product Analytics

**Objective:** Use Table API and SQL for declarative stream processing

**Task:**

1. Create table from socket stream of product views
2. Use SQL to calculate:
    - Top 5 most viewed products per hour
    - View-to-purchase conversion rate
3. Output results to console table

**Expected Skills:** StreamTableEnvironment, CREATE TABLE, TUMBLE windows in SQL, GROUP BY

***

## **Package: `async_io` - Asynchronous I/O**

### Exercise 10: User Profile Enrichment

**Objective:** Enrich stream data with external database lookups asynchronously

**Task:**

1. Read transaction stream: `transactionId,userId,amount`
2. Asynchronously lookup user profile from external REST API
3. Enrich transaction with user details (name, country, membershipLevel)
4. Calculate total spending per membership level

**Expected Skills:** AsyncDataStream, AsyncFunction, ResultFuture, timeout handling

***

## **General Challenge Exercise**

### Exercise 11: Real-Time Ride-Sharing Analytics Platform

**Objective:** Combine multiple Flink concepts

**Task:**
Build a complete pipeline:

1. Ingest ride requests and driver locations
2. Match rides to nearby drivers (< 2km)
3. Track ride status (requested → matched → started → completed)
4. Calculate metrics:
    - Average wait time per city
    - Driver utilization rate
    - Peak demand hours
5. Detect anomalies (rides taking too long)
6. Output to dashboards via Kafka

**Expected Skills:** Multi-stream processing, stateful operations, windowing, joins, side outputs, connectors

***
