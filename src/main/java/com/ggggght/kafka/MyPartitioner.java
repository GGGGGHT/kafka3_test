package com.ggggght.kafka;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class MyPartitioner implements Partitioner {

  @Override public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1,
      Cluster cluster) {
    return o1.toString().contains("ght") ? 0 : 1;
  }

  @Override public void close() {

  }

  @Override public void configure(Map<String, ?> map) {

  }
}
