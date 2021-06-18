package com.target.generalize.kafkaBase;

import com.target.generalize.pojo.FilterPojo;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

public class GeneraliseloadProducer {
    public static FilterPojo f;
    public static String topic;


    public  static FlinkKafkaProducer010 creatkafkaProducerInstance(Collection<FilterPojo> inputargs ) throws  Exception{

        Iterator<FilterPojo> iterator = inputargs.iterator();
        Properties kafkaProps = new Properties();
        while (iterator.hasNext()) {
            System.out.println("Entered consumer");
            f=iterator.next();
            System.out.println("the value of kafka consumer :"+ f.getBootstrap()+":"+f.getTopic());

        }

        try {

            FlinkKafkaProducer010 kafkaInstance = new FlinkKafkaProducer010<>(f.getBootstrap(), f.getTopic(),new SimpleStringSchema());
            return kafkaInstance;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }

    }
}



