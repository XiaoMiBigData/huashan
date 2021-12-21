package com.Jackson.kafka;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;


import java.util.Properties;

/**
 * Author : Jackson
 * Version : 2020/4/30 & 1.0
 * <p>
 * <p>
 * 输入topic:log
 * MOVIE_RATING_PREFIX:1|18|4.5|20190430
 * <p>
 * 输出：topic：recom
 * 1|18|4.5|20190430
 */

public class Application {
    public static void main(String[] args) {



        String brokers = "192.168.1.126:9092";
        String zookeeper = "192.168.1.126:2181";

        String fromTopic = "log";
        String toTopic = "recom";

        Properties settings = new Properties();

        settings.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");//相当于 setname
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,zookeeper);

        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,MyEventTimeExtractor.class.getName());

        StreamsConfig config = new StreamsConfig(settings);

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE",fromTopic)
                .addProcessor("PROCESS",()->new LogProcessor(),"SOURCE")
                .addSink("SINK",toTopic,"PROCESS");

        KafkaStreams streams = new KafkaStreams(builder,config);
        streams.start();


    }
}
