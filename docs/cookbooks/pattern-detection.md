---
title: Pattern Detection with CEP
nav-id: pattern-detection
nav-parent_id: cookbooks
nav-pos: 2
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}

## Problem

Many use cases require detecting a pattern in data streams:

  - detect authentication brute force (multiple failed authentication attempts in a raw);
  - detect unusual activity in application/network logs to anticipate a potential attack;
  - detect users starting a process in an application and stopping in the middle (new signups, etc.);
  - detect fraudulent financial operations,
  - etc.

Detecting such patterns may require complex developments. Flink's Complex Event Processing (CEP) library has been built to solve these specific problems and simplify these developments. We define a pattern with a few lines of code and Flink's engine applies the pattern detection on the data stream to match every relevant event.

## Use case

When a bank's client uses his credit card, the terminal asks first for authorization to the bank. The bank must as fast as possible evaluate if this operation seems to be legitimate or not. An operation is considered legitimate if it doesn't match any pattern of fraudulent activity. If the operation is considered illegitimate the bank will block it and maybe notify the card owner as soon as possible.

The pattern we are going to illustrate is two operations made at a distance that the client couldn't have traveled to in the amount of time separating these operations.

To keep things simple in this cookbook we will consider that bank's clients cannot travel faster than 200 Km/h. If two operations are made at a distance requiring that the client traveled faster than 200 Km/h the pattern will match the operation and raise an alert.

## Implementation

### 1. Add the CEP library

The CEP library must be added as a dependency. Add in your pom.xml

{% highlight xml %}

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-cep_2.10</artifactId>
  <version>1.3.2</version>
</dependency>

{% endhighlight %}

Your IDE should detect the new dependency automatically. You can also execute in your console `mvn clean package` if you do not use any.


### 2. Create a pattern

First, we recommend you to read the [CEP Flink Documentation]({{ site.baseurl }}/dev/libs/cep.html) to discover all possible pattern operations and conditions available in the CEP.

Our pattern is the following: two credit card operations in a raw have been made at an unreachable distance. Using credit card operations' timestamps and the distance between the cities, we calculate the speed necessary to travel between these two places in that amount of time. If it exceeds 200 Km/h, we consider that it is a fraudulent operation and we match it.

Our pattern starts with any credit card operation, so we do not filter the *begin* operator with a *where* clause.

To calculate the traveling speed, we need information about the current credit card operation and the previous one. Using an *IterativeCondition*, we can access retrieve this information from the context.

We use the *next* operator to match a credit card operation with the following one.

{% highlight java %}

Pattern<CreditCardOperation, CreditCardOperation> fraudulentOperationPattern =
  Pattern.<CreditCardOperation>begin("start")
    .next("unreachableDistance").where(
      new IterativeCondition<CreditCardOperation>() {
        @Override
        public boolean filter(CreditCardOperation currentOperation, Context<CreditCardOperation> ctx) throws Exception {
          Iterator eventsIterator = ctx.getEventsForPattern("start").iterator();
          if (eventsIterator.hasNext()) {
            CreditCardOperation previousOperation = (CreditCardOperation) eventsIterator.next();
            return !DistanceCalculator.isReachableDistance(previousOperation, currentOperation, 200); // 200 Km/h
          }
          return false;
        }
    });

{% endhighlight %}

### 3. Apply pattern and match patterns

We now have a defined the pattern and we can apply it on a stream to match patterns. We will also define a time window in which the pattern must occur. In our case, we will use 24 hours.

{% highlight java %}

// Simulated credit card operations stream
DataStream<CreditCardOperation> creditCardOperationDataStream = creditCardOperationSource(env);

PatternStream<CreditCardOperation> patternStream = CEP.pattern(creditCardOperationDataStream, fraudulentOperationPattern.within(Time.hours(24)));

{% endhighlight %}

The returned PatternStream contains credit card operations which have matched our pattern. In this example, they will be the potential fraudulent ones that need to be blocked and raise an alert for.

We need to select credit card operations in the PatternStream to generate an alert. In this example, we will only print the fraudulent operations but in a real case, we'll have to block the operation.

{% highlight java %}

DataStream<Alert> alerts = patternStream.select(new PatternSelectFunction<CreditCardOperation, Alert>() {
  @Override
  public Alert select(Map<String, List<CreditCardOperation>> pattern) throws Exception {
    Iterator patternIterator = pattern.get("unreachableDistance").iterator();

    if (patternIterator.hasNext()) {
      return Alert.createFromOperation((CreditCardOperation) patternIterator.next());
    }
    return null;
  }
});

alerts.print();

{% endhighlight %}

## Full source code

{% highlight java %}

package org.apache.flink.cookbook;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;

import java.util.*;

public class PatternDetection {

    public static final List<Integer> userIds = Arrays.asList(1, 2, 3);

    public static void main(String args[]) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Pattern<CreditCardOperation, CreditCardOperation> fraudulentOperationPattern =
            // We do not add any where clause on "start", we want to capture every operation
            Pattern.<CreditCardOperation>begin("start")
                // We want to compare the current operation with the previous one. We use the next operator
                .next("unreachableDistance").where(
                new IterativeCondition<CreditCardOperation>() {
                    @Override
                    public boolean filter(CreditCardOperation currentOperation, Context<CreditCardOperation> ctx) throws Exception {
                        // We want to check if the compare if the current operation is at a reachable distance. If not, it's
                        // a match and it will raise an alert.
                        Iterator eventsIterator = ctx.getEventsForPattern("start").iterator();
                        if (eventsIterator.hasNext()) {
                            CreditCardOperation previousOperation = (CreditCardOperation) eventsIterator.next();
                            // We match every operation where the user would have had to travel faster than
                            // 200 km/h to be able to make them.
                            return !DistanceCalculator.isReachableDistance(previousOperation, currentOperation, 200);
                        }
                        return false;
                    }
                });

        // Simulated credit card operations stream
        DataStream<CreditCardOperation> creditCardOperationDataStream = creditCardOperationSource(env);

        PatternStream<CreditCardOperation> patternStream = CEP.pattern(creditCardOperationDataStream, fraudulentOperationPattern.within(Time.hours(24)));
        DataStream<Alert> alerts = patternStream.select(new PatternSelectFunction<CreditCardOperation, Alert>() {
            @Override
            public Alert select(Map<String, List<CreditCardOperation>> pattern) throws Exception {
                Iterator patternIterator = pattern.get("unreachableDistance").iterator();

                if (patternIterator.hasNext()) {
                    return Alert.createFromOperation((CreditCardOperation) patternIterator.next());
                }
                return null;
            }
        });

        alerts.print();

        env.execute("Pattern detection");

    }

    /**
     * DataStream source generating CreditCardOperations.
     * @param env
     * @return a DataStream of CreditCardOperations
     */
    public static DataStream<CreditCardOperation> creditCardOperationSource(StreamExecutionEnvironment env) {
        List<CreditCardOperation> ccOps = new ArrayList<CreditCardOperation>();
        DateTime beginTS = DateTime.now();

        // User 1
        ccOps.add(new CreditCardOperation(1, 100.0f, beginTS.plusMinutes(5).getMillis(), "BERLIN"));
        ccOps.add(new CreditCardOperation(1, 68.12f, beginTS.plusMinutes(15).getMillis(), "BERLIN"));
        // 875 km in 45 min => Fraud
        ccOps.add(new CreditCardOperation(1, 200.0f, beginTS.plusMinutes(60).getMillis(), "PARIS"));

        // User 2
        ccOps.add(new CreditCardOperation(2, 33.99f, beginTS.plusMinutes(120).getMillis(), "PARIS"));
        ccOps.add(new CreditCardOperation(2, 224.50f, beginTS.plusMinutes(130).getMillis(), "PARIS"));
        // 395 km in 2 hours => OK
        ccOps.add(new CreditCardOperation(2, 348.85f, beginTS.plusMinutes(250).getMillis(), "LYON"));
        // 395 km in 10 min => Fraud
        ccOps.add(new CreditCardOperation(2, 125.85f, beginTS.plusMinutes(260).getMillis(), "PARIS"));

        return env.fromCollection(ccOps).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CreditCardOperation>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(CreditCardOperation operation) {
                return operation.timestamp;
            }
        });
    }


    /**
     * Credit card operation
     */
    public static class CreditCardOperation {
        public UUID operationId;
        public int user_id;
        public float amount;
        public long timestamp;
        public String city;

        public CreditCardOperation(int user_id, float amount, long timestamp, String city) {
            this.operationId = UUID.randomUUID();
            this.user_id = user_id;
            this.amount = amount;
            this.timestamp = timestamp;
            this.city = city;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("CreditCardOperation{");
            sb.append("operationId=").append(operationId);
            sb.append(", user_id=").append(user_id);
            sb.append(", amount=").append(amount);
            sb.append(", timestamp=").append(timestamp);
            sb.append(", city='").append(city).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Alert which will be pushed down the stream when a fraudulent operation is detected.
     */
    public static class Alert {
        public UUID operationId;
        public int user_id;
        public float amount;
        public long timestamp;
        public String city;

        public Alert(UUID operationId, int user_id, float amount, long timestamp, String city) {
            this.operationId = operationId;
            this.user_id = user_id;
            this.amount = amount;
            this.timestamp = timestamp;
            this.city = city;
        }

        public static Alert createFromOperation(CreditCardOperation operation) {
            return new Alert(operation.operationId, operation.user_id, operation.amount, operation.timestamp, operation.city);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("");
            sb.append("ALERT! Operation [").append(operationId).append("] has been blocked. \t");
            sb.append("{user_id=").append(user_id);
            sb.append(", amount=").append(amount);
            sb.append(", timestamp=").append(timestamp);
            sb.append(", city='").append(city).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Calculate distance and travel speed between 2 credit card operations
     */
    public static class DistanceCalculator {
        private static final HashMap<String, Tuple2<Double, Double>> cities;

        static {
            cities = new HashMap<>();
            cities.put("PARIS", new Tuple2<>(48.8573, 2.3493));
            cities.put("LYON", new Tuple2<>(45.7580, 4.8330));
            cities.put("BERLIN", new Tuple2<>(52.5181, 13.3896));
        }

        /**
         * Check if the the distance between 2 operations is reachable in the amount of time available during
         * these 2 operations
         *
         * @param operation1 the first credit card operation (oldest)
         * @param operation2 the last credit card operation (newest)
         * @param maxSpeed maximum speed in Km/h the person can travel at
         * @return True if the distance is reachable
         */
        public static boolean isReachableDistance(CreditCardOperation operation1, CreditCardOperation operation2, int maxSpeed) {
            float timeInHours = ((float) (operation2.timestamp - operation1.timestamp) / 3600000);
            int distance = distanceBetween(operation1.city, operation2.city);
            return (distance / timeInHours) <= maxSpeed;
        }

        /**
         * Calculate the distance in Km between 2 cities
         * @param cityCode1 first city
         * @param cityCode2 second city
         * @return distance in Km
         */
        private static int distanceBetween(String cityCode1, String cityCode2) {
            Tuple2<Double, Double> city1 = cities.get(cityCode1);
            Tuple2<Double, Double> city2 = cities.get(cityCode2);

            if (city1 == null || city2 == null) {
                return -1;
            }

            return (int) distance(city1.f0, city1.f1, city2.f0, city2.f1);
        }

        /**
         * Calculate the distance between 2 geocoordinates
         * @param lat1 latitude geocoordinates 1
         * @param lon1 longitude geocoordinates 1
         * @param lat2 latitude geocoordinates 2
         * @param lon2 longitude geocoordinates 2
         * @return distance in Km
         */
        private static double distance(double lat1, double lon1, double lat2, double lon2) {
            double theta = lon1 - lon2;
            double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
            dist = Math.acos(dist);
            dist = rad2deg(dist);
            dist = dist * 111;
            return (dist);
        }


        private static double deg2rad(double deg) {
            return (deg * Math.PI / 180.0);
        }

        private static double rad2deg(double rad) {
            return (rad * 180 / Math.PI);
        }
    }


{% endhighlight %}
