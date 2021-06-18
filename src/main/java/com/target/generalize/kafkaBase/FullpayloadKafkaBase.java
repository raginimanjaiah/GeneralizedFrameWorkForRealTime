package com.target.generalize.kafkaBase;


public class FullpayloadKafkaBase {

	private String kafkaAddress;
	private String topic;

	public String getTopic() {
		return topic;
	}

	public String getKafkaAddress() {
		return kafkaAddress;
	}

	public FullpayloadKafkaBase setKafkaAddress(String kafkaAddress) {
		this.kafkaAddress = kafkaAddress;
		return this;
	}

	public FullpayloadKafkaBase setTopic(String topic) {
		this.topic = topic;
		return this;
	}


	public  FullpayloadKafkaBase(String topic, String kafkaAddress) {
		this.setKafkaAddress(kafkaAddress);
		this.setTopic(topic);

	}

}
