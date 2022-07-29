package com.devslang.kafka.retryable_consumer.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailureRecordCountCacheInMem<K> implements FailureRecordCache<K, Long> {

	private final static Logger log = LoggerFactory.getLogger(FailureRecordCountCacheInMem.class);

	private Map<K, AtomicLong> cache = new ConcurrentHashMap<>();

	private final Lock lock = new ReentrantLock();

	public Long add(K key) {

		lock.lock();
		AtomicLong counter = cache.get(key);
		if (counter == null) {
			cache.putIfAbsent(key, new AtomicLong(0));
			counter = cache.get(key);
		}
		long count = counter.incrementAndGet();
		log.info("Incrementing count for : {}, to : {}", key, count);
		lock.unlock();
		return count;

	}

	public void remove(K key) {

		lock.lock();
		AtomicLong counter = cache.get(key);
		if (counter == null) {
			cache.putIfAbsent(key, new AtomicLong(0));
			counter = cache.get(key);
		}
		Long count = counter.decrementAndGet();

		log.info("Decrementing count for : {}, to : {}", key, count);

		lock.unlock();
	}

	public Long get(K key) {

		lock.lock();
		AtomicLong atomicLong = cache.get(key);
		Long count = null;
		if (atomicLong != null) {
			count = atomicLong.longValue();
		}
		lock.unlock();
		return count;
	}

	@Override
	public void reset(K key) {

		lock.lock();
		cache.remove(key);
		lock.unlock();

	}

}
