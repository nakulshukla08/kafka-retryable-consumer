package com.devslang.kafka.retryable_consumer.core;

import java.util.UUID;

import com.devslang.kafka.retryable_consumer.util.UuidUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.converter.MessageConversionException;

public class RetryTopicConsumer<K, V> extends FailureAwareMessageListener<K, V, Long> {

	private static final Logger log = LoggerFactory.getLogger(RetryTopicConsumer.class);

	private static final Long RETRY_FAILURE_THRESHOLD = 2L;
	
	private static final Long NACK_TIMEOUT = 100L;

	private IMessageProcessor<K, V> messageProcessor;

	private FailureRecordCache<K, Long> cache;

	private FailureRecordCache<UUID, Long> falloutControlCache = new FailureRecordCountCacheInMem<>();

	private FalloutHandler<K, V> falloutHandler = new InMemoryFalloutHandler<>();

	public RetryTopicConsumer(IMessageProcessor<K, V> messageProcessor, FailureRecordCache<K, Long> cache,
			KafkaConsumerInfo consumerInfo) {

		super(messageProcessor, cache, consumerInfo);
		this.messageProcessor = messageProcessor;
		this.cache = cache;
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, Acknowledgment acknowledgment) {

		log.info("Message arrived : {}", data);

		K messageKey = data.key();
		V messageValue = data.value();
		try {

			// process message
			messageProcessor.process(messageKey, messageValue);

			if (cache.get(messageKey) != null && cache.get(messageKey) >= 1) {
				cache.remove(messageKey);
			}

			acknowledgment.acknowledge();

		}
		catch (MessageConversionException mce) {
			log.error("message conversion exception for event : {}", messageValue);
			log.error("Acknowledging despit of error to skip | ", mce);
			acknowledgment.nack(NACK_TIMEOUT);
		}
		catch (Exception exception) {
			UUID messageId = null;

			Iterable<Header> mesageIdHeader = data.headers()
				.headers(AbstractEventPublisher.MESSAGE_ID_HEADER);
			if (mesageIdHeader != null && mesageIdHeader.iterator()
				.hasNext()) {
				messageId = UuidUtils.asUuid(mesageIdHeader.iterator()
					.next()
					.value());
			}

			if (messageId == null) {
				log.warn("Message was published without an ID, will push to fallouts");
				falloutHandler.handle(messageKey, messageValue);
				acknowledgment.acknowledge();
				return;
			}

			Long retryFailureCount = falloutControlCache.get(messageId);
			if (retryFailureCount != null && retryFailureCount >= RETRY_FAILURE_THRESHOLD) {
				// call fallout handler
				falloutHandler.handle(messageKey, messageValue);
				falloutControlCache.reset(messageId);
				acknowledgment.acknowledge();
				return;

			}

			falloutControlCache.add(messageId);

			log.error(String.format("Consumer : %s, Exception occurred while processing vehicle event : %s", getConsumerId(),
				messageValue),
				exception);
			acknowledgment.nack(NACK_TIMEOUT);
		}

	}

	public FailureAwareMessageListener<K, V, Long> initializeConsumerAndGet() {

		// TODO Auto-generated method stub
		return super.initializeConsumerAndGet(this.consumerInfo.getTopic());
	}

}
