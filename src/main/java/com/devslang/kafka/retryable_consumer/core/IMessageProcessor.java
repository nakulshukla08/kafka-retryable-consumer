package com.devslang.kafka.retryable_consumer.core;


/**
 * 
 * Handler for processing incoming Kafka messages, which gets called from FailureAwareMessageListener
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <V>
 */
public interface IMessageProcessor<K,V> {

	void process(K key, V value);
}
