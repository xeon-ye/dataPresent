package com.tiger.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;

public class KafkaAppender extends ConsoleAppender<ILoggingEvent> {

    private KafkaTemplate kafkaTemplate;

    @Override
    public void start() {
        super.start();
        Map<String, Object> props = new HashMap();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        try{
        	kafkaTemplate = new KafkaTemplate(new DefaultKafkaProducerFactory(props));
        	if(kafkaTemplate!=null){
                kafkaTemplate.send("test", "连接到Kafka。。。。。。。");// 先连接一遍，如果去掉可能报   Failed to update metadata after 60000 ms
        	}
        }catch(Exception e){
        	System.out.print("kafka连接失败："+e.toString());
        }
        
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
    	try{
    		if(kafkaTemplate!=null){
    			kafkaTemplate.send("test", eventObject.getFormattedMessage());
    		}
    	}catch(Exception e){
    		System.out.print("kafka连接失败："+e.toString());
        }
    }
}