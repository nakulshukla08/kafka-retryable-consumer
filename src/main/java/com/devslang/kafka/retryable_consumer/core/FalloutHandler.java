package com.devslang.kafka.retryable_consumer.core;


/**
 * A handler that gets called when the retries are exhausted in RetryTopicConsumer.
 * The Kafka message is passed in a key-value pair format, which could be handled as per the
 * custom logic provided by the application.
 * @param <K>
 * @param <V>
 */
public interface FalloutHandler<K, V> {

	public void handle(K key, V value);

}
