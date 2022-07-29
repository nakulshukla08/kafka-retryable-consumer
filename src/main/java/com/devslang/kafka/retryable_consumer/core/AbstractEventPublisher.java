package com.devslang.kafka.retryable_consumer.core;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import com.devslang.kafka.retryable_consumer.util.UuidUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class AbstractEventPublisher<K, V> {

	public static final String MESSAGE_ID_HEADER = "X-Message-Id";

	private final ExecutorService executorService;
	private final KafkaTemplate<K, V> kafkaTemplate;

	public AbstractEventPublisher(@NonNull final ExecutorService executorService,
			@NonNull final KafkaTemplate<K, V> kafkaTemplate) {

		this.executorService = executorService;
		this.kafkaTemplate = kafkaTemplate;
	}

	final void publish(K key, @NonNull V message, String topic, ListenableFutureCallback<SendResult<K, V>> callback) {

		Event event = new Event(topic, key, message, callback);
		executorService.submit(new PublishingTask(event, kafkaTemplate));
	}

	enum Status {
		SUBMITTED, STARTED, PUBLISHED, FAILED
	}

	class PublishingTask implements Callable<Status> {
		private final Event event;
		private final KafkaTemplate<K, V> kafkaTemplate;
		private Status status;

		PublishingTask(Event event, KafkaTemplate<K, V> kafkaTemplate) {

			this.event = event;
			this.kafkaTemplate = kafkaTemplate;
			this.status = Status.SUBMITTED;
		}

		@Override
		public Status call() throws Exception {

			status = Status.STARTED;
			var record = createProducerRecord();
			kafkaTemplate.send(record)
				.addCallback(new ListenableFutureCallback<>() {
					@Override
					public void onFailure(@NonNull Throwable ex) {

						status = Status.FAILED;
						if (event.callback != null) {
							event.callback.onFailure(ex);
						}
					}

					@Override
					public void onSuccess(SendResult<K, V> result) {

						status = Status.PUBLISHED;
						if (event.callback != null) {
							event.callback.onSuccess(result);
						}
					}
				});

			return status;
		}

		private ProducerRecord<K, V> createProducerRecord() {

			var record = new ProducerRecord<K, V>(event.getTopic(), event.getKey(), event.getPayload());
			UUID uuid = UUID.randomUUID();
			record.headers()
				.add(MESSAGE_ID_HEADER, UuidUtils.asBytes(uuid));
			return record;
		}
	}

	class Event {
		private final String topic;
		private final K key;
		private final V payload;
		private ListenableFutureCallback<SendResult<K, V>> callback;

		Event(@NonNull String topic, K key, @NonNull V payload, ListenableFutureCallback<SendResult<K, V>> callback) {

			this.topic = topic;
			this.key = key;
			this.payload = payload;
			this.callback = callback;
		}

		protected String getTopic() {

			return topic;
		}

		protected K getKey() {

			return key;
		}

		protected V getPayload() {

			return payload;
		}

		protected ListenableFutureCallback<SendResult<K, V>> getCallback() {

			return callback;
		}
	}
}
