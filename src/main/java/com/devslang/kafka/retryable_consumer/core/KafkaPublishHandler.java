package com.devslang.kafka.retryable_consumer.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class KafkaPublishHandler<K, V> {

	private static final Logger log = LoggerFactory.getLogger(KafkaPublishHandler.class);

	private FailureRecordCountCacheInMem<K> cache;

	private AbstractEventPublisher<K, V> publisher;

	public KafkaPublishHandler(FailureRecordCountCacheInMem<K> cache, AbstractEventPublisher<K, V> publisher) {

		super();
		this.cache = cache;
		this.publisher = publisher;
	}

	public void redirectToRetryTopic(K id, V message, String retryTopic) {

		publisher.publish(id, message, retryTopic, new ListenableFutureCallback<SendResult<K, V>>() {
			@Override
			public void onFailure(Throwable ex) {

				log.error("Error in sending event to kafka", ex);
			}

			@Override
			public void onSuccess(SendResult<K, V> result) {

				log.info("Publishing the message : {}, at topic : {}", result.getProducerRecord()
					.value(), result.getProducerRecord()
						.topic());
				incrementFailedCountInCache(result);

			}

			private void incrementFailedCountInCache(SendResult<K, V> result) {

				K messageId = result.getProducerRecord()
					.key();

				cache.add(messageId);
			}
		});

	}

}
