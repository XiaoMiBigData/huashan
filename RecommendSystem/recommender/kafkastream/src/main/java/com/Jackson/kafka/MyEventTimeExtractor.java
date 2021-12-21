package com.Jackson.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Author : Jackson
 * Version : 2020/4/30 & 1.0
 */

public class MyEventTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        return System.currentTimeMillis();
    }
}
