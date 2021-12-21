package com.Jackson.kafka;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Author : Jackson
 * Version : 2020/4/30 & 1.0
 */

public class LogProcessor implements Processor<byte[],byte[]>{

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {

        this.context = processorContext;

    }

    @Override
    public void process(byte[] bytes, byte[] line) {
        //数据处理逻辑
        String input = new String(line);

        //MOVIE_RATING_PREFIX:1|20|5.0|1564412038

        if (input.contains("MOVIE_RATING_PREFIX:")){
            input = input.split("MOVIE_RATING_PREFIX:")[1].trim();

            System.out.println("proccess data");

            context.forward("logProcessor".getBytes(),input.getBytes());
        }


    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
