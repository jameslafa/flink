---
title: Join Streams
nav-id: join-streams
nav-parent_id: cookbooks
nav-pos: 1
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

Like in relational databases, we avoid duplicating data in streams to improve performances. However, we need some time to add external context to data flowing in a stream. In a relational database, we would JOIN two tables. In Flink, we join two streams based on a key to generate a stream of enriched data.

A common pattern is Change Data Capture (CDC). We capture data modifications pushed in a stream and save them in the state. When data flows in the second stream, we look up the state to get the related context and enrich the flowing data. This technique makes it possible to execute a lookup at very low latency in the state instead of a slow lookup in an external source like a database.


## Use case

To illustrate this technique, we take the example of a company running multiple advertisement campaigns which would like to display analytics in a dashboard.

**Streams:**

- A new *AdClick* is pushed in *AdClickStream* each time a user clicks on an ad;
- A new *AdCampaign* is pushed in *AdCampaignStream* each time the marketing team creates or updates an ad;
- A new *EnrichedAdClick* is pushed in the joined stream *EnrichedAdClickStream* for each *AdClick* flowing in *AdClickStream*. To keep things simple *EnrichedAdClick* is just an *AdClick* enriched with the *campaignName*.


## Implementation

### 1. Connect streams

We need to connect streams before joining them. Two streams are connected via a common key, here *AdCampaign.id* and *AdClick.campaignId*.

{% highlight java %}

DataStream<EnrichedAdClick> joinedStream = adClicksStream
                                            .keyBy("campaignId")
                                            .connect(adCampaignStream.keyBy("id"))     

{% endhighlight %}


### 2. Join streams

In our case, we want to enrich data flowing in *AdClickStream* with data flowing in *AdCampaignStream*. Therefore, we must capture *AdCampaign* changes in the state which will later be used to look up *AdCampaigns* details.

We apply a [flatMap]({{ site.baseurl }}/dev/datastream_api.html#datastream-transformations) operator using a [RichCoFlatMapFunction]({{ site.baseurl }}/api/java/org/apache/flink/streaming/api/functions/co/RichCoFlatMapFunction.html) on the connected streams.


#### 2.1 Create a new RichCoFlatMapFunction

Let's create a new function extending RichCoFlatMapFunction. We must specific three type parameters:

 1. the data type flowing in the first stream: *AdClick*
 2. the data type flowing in the second stream: *AdCampaign*
 3. the data type which will be collected by the collector and pushed downstream: *EnrichedAdClick*

{% highlight java %}

public static class StreamJoinFunction extends RichCoFlatMapFunction<AdClick, AdCampaign, EnrichedAdClick>

{% endhighlight %}

#### 2.2 Define the state

We save AdCampaign information in a MapState. MapState has a structure similar to an HashMap: <key, value>. In our case, the key is the *AdCampaign.id* and the *AdCampaign* is stored as the value.

{% highlight java %}

private MapState<Integer, AdCampaign> adCampainState;

{% endhighlight %}

The state must be defined in the method *open* which we override.

{% highlight java %}

@Override
public void open(Configuration parameters) throws Exception {
    adCampainState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, AdCampaign>(
        "adCampaigns", Integer.class, AdCampaign.class));
}

{% endhighlight %}

#### 2.3 Process elements from both streams

Two more methods must be overridden: *flatMap1* and *flatMap2*.

*flatMap2* is executed when new elements are flowing in the second stream: *AdCampaignStream*. We only need to insert/update the *AdCampaign* in the MapState.

{% highlight java %}

@Override
public void flatMap2(AdCampaign adCampaign, Collector<EnrichedAdClick> collector) throws Exception {
    adCampainState.put(adCampaign.id, adCampaign);
}

{% endhighlight %}

*flapMap1* is executed when new elements flow in the first stream *AdClickStream*. For each flowing *AdClick* we read the corresponding *AdCampaign* information from the MapState and create a new *EnrichedAdClick* that we push downstream via the collector.

{% highlight java %}

@Override
public void flatMap1(AdClick adClick, Collector<EnrichedAdClick> collector) throws Exception {
    AdCampaign currentAdCampaign = adCampainState.get(adClick.campaignId);
    String campaignName = (currentAdCampaign != null) ? currentAdCampaign.campaignName : "Unknown campaign";

    EnrichedAdClick enrichedAdClick = new EnrichedAdClick(adClick, campaignName);
    collector.collect(enrichedAdClick);
}

{% endhighlight %}

That's it! The flatMap operator returns now a stream of *EnrichedAdClick*. You can apply other operators of the [DataStreamApi]({{ site.baseurl }}/dev/datastream_api.html).


## Full source code

{% highlight java %}

package org.apache.flink.cookbook;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Random;


public class StreamJoin {

    static final int adCampaignNumber = 5;

    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Simulated AdCampaign stream
        DataStream<AdCampaign> adCampaignStream = adCampaignSource(env);

        // Simulated AdClick stream
        DataStream<AdClick> adClicksStream = adClickSource(env);

        // Connect streams by keys and join them
        DataStream<EnrichedAdClick> joinedStream = adClicksStream
                .keyBy("campaignId")
                .connect(adCampaignStream.keyBy("id"))
                .flatMap(new StreamJoinFunction());

        // Print the EnrichedAdClick
        joinedStream.print();

        env.execute("Stream join");
    }

    /**
     * DataStream source generating AdCampaigns.
     * It generates first a series of campaigns and keep sending random updates to existing campaigns.
     *
     * @param env
     * @return a DataStream of AdCampaigns
     */
    public static DataStream<AdCampaign> adCampaignSource(StreamExecutionEnvironment env) {
        DataStream<AdCampaign> adCampaignStream = env.addSource(new SourceFunction<AdCampaign>() {
            private boolean keepStreaming = true;

            @Override
            public void run(SourceContext<AdCampaign> sourceContext) throws Exception {
                AdCampaign[] adCampaigns = new AdCampaign[adCampaignNumber];

                // Generate initial AdCampaigns
                for (int i = 0; i < adCampaignNumber; i++) {
                    Thread.sleep(1);

                    adCampaigns[i] = new AdCampaign(i + 1);
                    sourceContext.collect(adCampaigns[i]);
                }

                // Push changes to existing AdCampaigns
                Random random = new Random();

                while (keepStreaming) {
                    Thread.sleep(random.nextInt(5000));
                    int campaignIndex = random.nextInt(adCampaignNumber);
                    adCampaigns[campaignIndex].updateCampaignVersion();
                    sourceContext.collect(adCampaigns[campaignIndex]);
                }
            }

            @Override
            public void cancel() {
                keepStreaming = false;
            }
        });

        return adCampaignStream;
    }

    /**
     * DataStream source generating AdClicks.
     *
     * @param env
     * @return DataStream of AdClicks
     */
    public static DataStream<AdClick> adClickSource(StreamExecutionEnvironment env) {
        DataStream<AdClick> adClickStream = env.addSource(new SourceFunction<AdClick>() {
            private boolean keepStreaming = true;

            @Override
            public void run(SourceContext<AdClick> sourceContext) throws Exception {
                Random random = new Random();

                long initialTimestamp, timestampDiff;
                initialTimestamp = new Date().getTime();

                int lastAdClickId = 0;

                while (keepStreaming) {
                    Thread.sleep(10 + random.nextInt(300));
                    timestampDiff = new Date().getTime() - initialTimestamp;
                    sourceContext.collectWithTimestamp(new AdClick(lastAdClickId++,
                            new Random().nextInt(adCampaignNumber) + 1), timestampDiff);
                    sourceContext.emitWatermark(new Watermark(timestampDiff));
                }
            }

            @Override
            public void cancel() {
                keepStreaming = false;
            }
        });
        return adClickStream;
    }

    /**
     * RichCoFlatMapFunction used to join AdClicks and AdCampaigns and generate EnrichedAdClicks
     */
    public static class StreamJoinFunction extends RichCoFlatMapFunction<AdClick, AdCampaign, EnrichedAdClick> {

        /**
         * AdCampaigns information are kept in the state
         */
        private MapState<Integer, AdCampaign> adCampainState;


        /**
         * Initialize the state used to store AdCampaigns information.
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            adCampainState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, AdCampaign>(
                    "adCampaigns", Integer.class, AdCampaign.class));
        }

        /**
         * Process every element of the first stream: AdClicks.
         * The function enrich the incoming AdClick with the AdCampaign information present in the state.
         *
         * @param adClick   incoming AdClicks
         * @param collector outcoming EnrichedAdClicks
         * @throws Exception
         */
        @Override
        public void flatMap1(AdClick adClick, Collector<EnrichedAdClick> collector) throws Exception {
            AdCampaign currentAdCampaign = adCampainState.get(adClick.campaignId);
            String campaignName = (currentAdCampaign != null) ? currentAdCampaign.campaignName : "Unknown campaign";

            EnrichedAdClick enrichedAdClick = new EnrichedAdClick(adClick, campaignName);
            collector.collect(enrichedAdClick);
        }

        /**
         * Process every element of the second stream: AdCampaigns.
         * AdCampaigns information are stored in the State.
         *
         * @param adCampaign incoming AdCampaigns
         * @param collector  outcoming EnrichedAdClicks (unused here, we only update the state)
         * @throws Exception
         */
        @Override
        public void flatMap2(AdCampaign adCampaign, Collector<EnrichedAdClick> collector) throws Exception {
            adCampainState.put(adCampaign.id, adCampaign);
        }
    }

    public static class AdCampaign {
        public int id;
        public String campaignName;
        public Date updated_at;

        public AdCampaign() {
        }

        public AdCampaign(int id) {
            this.id = id;
            this.updateCampaignVersion();
        }

        /**
         * Updates the campaignName by increasing the version
         */
        public void updateCampaignVersion() {
            updated_at = new Date();
            this.campaignName = "Campaign " + id + " v. " + updated_at.getTime();
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("AdCampaign{");
            sb.append("id=").append(id);
            sb.append(", campaignName='").append(campaignName).append('\'');
            sb.append(", updated_at=").append(updated_at);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class AdClick {
        public int id;
        public Date created_at;
        public int campaignId;

        public AdClick() {
        }

        public AdClick(int id, int campaignId) {
            this.id = id;
            this.created_at = new Date();
            this.campaignId = campaignId;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("AdClick{");
            sb.append("id=").append(id);
            sb.append(", created_at=").append(created_at);
            sb.append(", campaignId=").append(campaignId);
            sb.append('}');
            return sb.toString();
        }
    }

    /**
     * Enriched AdClick representation containing also campaign information (only campaignName in this example)
     */
    public static class EnrichedAdClick {
        public AdClick adClick;
        public String campaignName;

        public EnrichedAdClick(AdClick adClick, String campaignName) {
            this.adClick = adClick;
            this.campaignName = campaignName;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("EnrichedAdClick{");
            sb.append("id=").append(adClick.id);
            sb.append(", created_at=").append(adClick.created_at);
            sb.append(", campaignId=").append(adClick.campaignId);
            sb.append(", campaignName=").append(campaignName);
            sb.append('}');
            return sb.toString();
        }
    }
}

{% endhighlight %}
