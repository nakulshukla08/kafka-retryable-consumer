package com.devslang.kafka.retryable_consumer.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.converter.MessageConversionException;

public class CustomAckMessageListener<K, V> extends FailureAwareMessageListener<K, V, Long> {

	private static final Logger log = LoggerFactory.getLogger(CustomAckMessageListener.class);

	private static final String RETRY_TOPIC_SUFFIX = ".retry";

	private KafkaPublishHandler<K, V> publisher;

	private String retryTopic;

	public CustomAckMessageListener(IMessageProcessor<K, V> messageProcessor, KafkaPublishHandler<K, V> publisher,
			FailureRecordCache<K, Long> cache, KafkaConsumerInfo consumerInfo) {

		super(messageProcessor, cache, consumerInfo);
		this.publisher = publisher;
		this.retryTopic = consumerInfo.getTopic() + RETRY_TOPIC_SUFFIX;
	}

	public static <K, V> CustomAckMessageListener<K, V> createInstance(IMessageProcessor<K, V> messageProcessor,
			KafkaPublishHandler<K, V> publisher,
			FailureRecordCache<K, Long> cache, KafkaConsumerInfo consumerInfo) {

		return new CustomAckMessageListener<>(messageProcessor, publisher, cache, consumerInfo);

	}

	public String getRetryTopic() {

		return retryTopic;
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> consumerRecord, Acknowledgment acknowledgment) {

		log.info("Message arrived : {}", consumerRecord);
		K messageKey = consumerRecord.key();
		V messageValue = consumerRecord.value();
		try {

			if (cache.get(messageKey) != null && cache.get(messageKey) >= 1) {
				publisher.redirectToRetryTopic(messageKey, messageValue, getRetryTopic());
			}

			// process message
			messageProcessor.process(consumerRecord.key(), consumerRecord.value());

			// commit offset
			acknowledgment.acknowledge();

		}
		catch (MessageConversionException mce) {
			log.error("message conversion exception for event : {}", messageValue);
			log.error("Acknowledging despit of error to skip | ", mce);
		}
		catch (Exception exception) {
			log.error(String.format("Consumer : %s, Exception occurred while processing vehicle event : %s", getConsumerId(),
				messageValue),
				exception);
			publisher.redirectToRetryTopic(messageKey, messageValue, getRetryTopic());
			// acknowledgment.nack(2000);
			// return;
			// throw new RuntimeException(exception);
		}

		acknowledgment.acknowledge();
	}

	public FailureAwareMessageListener<K, V, Long> initializeConsumerAndGet() {

		// TODO Auto-generated method stub
		return super.initializeConsumerAndGet(this.consumerInfo.getTopic());
	}

	public FailureAwareMessageListener<K, V, Long> createRetryConsumer() {

		KafkaConsumerInfo consumerInfo = new KafkaConsumerInfo(this.getConsumerInfo()
			.getId() + "_retry", retryTopic, this.consumerInfo.getConsumerProps(), this.consumerInfo.getConcurrency());

		return new RetryTopicConsumer<>(messageProcessor, cache, consumerInfo).initializeConsumerAndGet();

	}

}
