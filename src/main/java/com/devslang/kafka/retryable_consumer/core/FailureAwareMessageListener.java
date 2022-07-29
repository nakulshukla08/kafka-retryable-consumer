package com.devslang.kafka.retryable_consumer.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

public class FailureAwareMessageListener<K, V, C> implements
		org.springframework.kafka.listener.AcknowledgingMessageListener<K, V> {

	protected IMessageProcessor<K, V> messageProcessor;

	protected FailureRecordCache<K, C> cache;

	protected KafkaConsumerInfo consumerInfo;

	public FailureAwareMessageListener(IMessageProcessor<K, V> messageProcessor, FailureRecordCache<K, C> cache,
			KafkaConsumerInfo consumerInfo) {

		super();
		this.messageProcessor = messageProcessor;
		this.cache = cache;
		this.consumerInfo = consumerInfo;
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, Acknowledgment acknowledgment) {

		// TODO Auto-generated method stub

	}
	
	protected FailureAwareMessageListener<K, V, C> initializeConsumerAndGet(String topic) {

		KafkaConsumerUtil.startOrCreateConsumers(topic, this, getConsumerInfo().getConcurrency(), getConsumerInfo()
			.getConsumerProps());
		return this;
	}

	public KafkaConsumerInfo getConsumerInfo() {

		return consumerInfo;
	}
	
	protected String getConsumerId() {
		return consumerInfo.getId();
	}
	
	protected String getTopic() {
		return consumerInfo.getTopic();
	}

}
