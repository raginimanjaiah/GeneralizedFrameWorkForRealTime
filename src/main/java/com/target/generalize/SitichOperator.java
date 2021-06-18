package com.target.generalize;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

public class SitichOperator {
    public SitichOperator(FlinkKafkaConsumer010<Tuple2<String, String>> loadKafkaConsumer) {
    }
}
