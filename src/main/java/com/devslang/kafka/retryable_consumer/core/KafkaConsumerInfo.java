package com.devslang.kafka.retryable_consumer.core;

import java.util.Map;

public class KafkaConsumerInfo {

	private String id;

	private String topic;

	private Map<String, Object> consumerProps;

	private int concurrency;

	public KafkaConsumerInfo(String id, String topic, Map<String, Object> consumerProps, int concurrency) {

		super();
		this.id = id;
		this.topic = topic;
		this.consumerProps = consumerProps;
		this.concurrency = concurrency;
	}

	public String getId() {

		return id;
	}

	public String getTopic() {

		return topic;
	}

	public Map<String, Object> getConsumerProps() {

		return consumerProps;
	}

	public void setConsumerProps(Map<String, Object> consumerProps) {

		this.consumerProps = consumerProps;
	}

	public int getConcurrency() {

		return concurrency;
	}

}
