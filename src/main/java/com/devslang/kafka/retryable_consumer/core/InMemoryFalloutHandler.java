package com.devslang.kafka.retryable_consumer.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryFalloutHandler<K, V> implements FalloutHandler<K, V> {

	private static final Logger log = LoggerFactory.getLogger(InMemoryFalloutHandler.class);

	private final Map<K, List<V>> falloutStorage = new HashMap<>();

	@Override
	public void handle(K key, V value) {

		log.info("message fallout! key : {}, value : {}", key, value);
		List<V> valueList = falloutStorage.get(key);
		if (valueList == null) {
			valueList = new LinkedList<>();

		}
		valueList.add(value);

		falloutStorage.put(key, valueList);

		log.info("Current fallout store keyset size : {}, value-size : {}", falloutStorage.size(), valueList.size());
		

	}

}
