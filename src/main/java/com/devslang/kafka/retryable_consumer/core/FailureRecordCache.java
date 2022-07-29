package com.devslang.kafka.retryable_consumer.core;


/**
 * A cache to store to keep a track of failed records based on which the messages are 
 * either re-directed to the retry topics or to fall-out stores.
 * @param <K> the key
 * @param <V> the value
 */
public interface FailureRecordCache<K, V> {

	public V add(K key);

	public void remove(K key);

	public V get(K key);
	
	public void reset(K key);

}
