package com.lymboy.apache.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author sairo
 * @date 19-2-21
 */
public class MyKafkaProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        //设置服务器地址
        props.put("bootstrap.servers", "cluster01:9092");
        //设置对所有请求应答
        props.put("acks", "all");
        //如果请求失败，kafka可以自动重试
        props.put("retries", 0);
        //设置缓冲区大小
        props.put("batch.size", 16384);
        //减少小于0的请求数
        props.put("linger.ms", 1);
        //buffer.memory控制生产者可用于缓冲的总内存量。
        props.put("buffer.memory", 33554432);
        //配置序列化器
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<>("test4",
                    Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();
    }
}
