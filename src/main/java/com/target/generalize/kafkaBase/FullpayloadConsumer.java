package com.target.generalize.kafkaBase;


import com.target.generalize.pojo.FilterPojo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

public class FullpayloadConsumer {
	private static FlinkKafkaConsumer010<Tuple2<String, String>> flinkKafkaConsumer;
	public static FilterPojo f;
	public static String topic;

	public static FlinkKafkaConsumer010<Tuple2<String, String>> createStringConsumerForTopic(Collection<FilterPojo> inputargs ) throws  Exception{
		Iterator<FilterPojo> iterator = inputargs.iterator();
		Properties kafkaProps = new Properties();
		while (iterator.hasNext()) {
			System.out.println("Entered");
			f=iterator.next();
			kafkaProps.setProperty("bootstrap.servers", f.getBootstrap());
			kafkaProps.setProperty("group.id", f.getGroupid());
			topic=f.getTopic();

			System.out.println("the value of kafka consumer :"+ f.getBootstrap()+":"+f.getTopic()+":"+f.getGroupid());

		}
		try {
			flinkKafkaConsumer = new FlinkKafkaConsumer010(topic, new CustomKafkaDeserializationSchema(), kafkaProps);
			flinkKafkaConsumer.setStartFromLatest();

		} catch (Exception e) {
			System.out.println("Exception in kafka Open");
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			String sStackTrace = sw.toString();
			System.out.println(sStackTrace);
		}
		return flinkKafkaConsumer;
	}
}

