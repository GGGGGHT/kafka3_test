package com.ggggght.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class CustomProducer {
  public static void main(String[] args) {
    Properties properties = new Properties();
    // 接连
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // k与v的序列化器
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // 使用自定义分区器
    properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.ggggght.kafka.MyPartitioner");
    // 1. 创建kafka生产者对象
    var producer = new KafkaProducer<String, String>(properties);
    // 2. 发送数据
    sendMsgWithOutCallback(producer);
    // 3.关闭资源
    producer.close();
  }

  static void sendMsgWithOutCallback(KafkaProducer<String, String> producer) {
    for (int i = 0; i < 5; i++) {
      producer.send(new ProducerRecord<>("first" + i, "hello"));
    }
  }

  static void sendMsgWithCallback(KafkaProducer<String, String> producer)  {
    for (int i = 0; i < 5; i++) {
      producer.send(new ProducerRecord<>("first" + i, "hello"), (recordMetadata, e) -> {
        // 发送完消息的回调
        if (e != null) {
          System.out.println(e.getMessage());
          return;
        }

        System.out.printf("topic: %s, partition: %d\n", recordMetadata.topic(),
            recordMetadata.partition());
      });
    }
  }
}
