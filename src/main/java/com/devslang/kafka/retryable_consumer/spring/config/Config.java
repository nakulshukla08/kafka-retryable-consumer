package com.devslang.kafka.retryable_consumer.spring.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.devslang.kafka.retryable_consumer.core.AbstractEventPublisher;
import com.devslang.kafka.retryable_consumer.core.ConcreteMessageProcessor;
import com.devslang.kafka.retryable_consumer.core.CustomAckMessageListener;
import com.devslang.kafka.retryable_consumer.core.FailureRecordCountCacheInMem;
import com.devslang.kafka.retryable_consumer.core.IMessageProcessor;
import com.devslang.kafka.retryable_consumer.core.KafkaConsumerInfo;
import com.devslang.kafka.retryable_consumer.core.KafkaPublishHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListener;

@EnableKafka
@Configuration
@EnableConfigurationProperties({ KafkaProperties.class })
public class Config {

	private static final int DEFAULT_THREADS = 1;
	
	public static final String TOPIC_FIREHOSE = "events.";

	@Value("${app.events.publisher.threads:1}")
	private int eventPublisherThreads;


	@Bean(name = "eventPublisherExecutor")
	@Scope("singleton")
	public ExecutorService getEventPublisherExecutor() {

		int threads = eventPublisherThreads;

		if (threads <= 0 || threads >= 50) {
			threads = DEFAULT_THREADS;
		}

		return Executors.newFixedThreadPool(threads);
	}

	@Bean
	public AbstractEventPublisher<String, String> retryEventPublisher(ExecutorService executorService,
			KafkaTemplate<String, String> kafkaTemplate) {

		return new AbstractEventPublisher<String, String>(executorService, kafkaTemplate);
	}

	@Bean
	public IMessageProcessor<String, String> messageProcessor() {

		return new ConcreteMessageProcessor();
	}

	@Bean
	public MessageListener<String, String> customAckMessageListener(IMessageProcessor<String, String> messageProcessor,
			KafkaPublishHandler<String, String> kafkaPublishHandler, FailureRecordCountCacheInMem<String> cache) {

		KafkaConsumerInfo consumerInfo = consumerInfo("customAckMessageListener", TOPIC_FIREHOSE+"Rental", consumerProps(),
			DEFAULT_THREADS);
		return CustomAckMessageListener.createInstance(messageProcessor, kafkaPublishHandler, cache, consumerInfo)
			.initializeConsumerAndGet();

	}

	@Bean
	public MessageListener<String, String> customRetryAckMessageListener(CustomAckMessageListener<String, String> mainListener) {

		return mainListener.createRetryConsumer();
	}

	@Bean
	public KafkaPublishHandler<String, String> kafkaPublishHandler(FailureRecordCountCacheInMem<String> cache,
			AbstractEventPublisher<String, String> retryEventPublisher) {

		return new KafkaPublishHandler<>(cache, retryEventPublisher);

	}

	@Bean
	public FailureRecordCountCacheInMem<String> cache() {

		return new FailureRecordCountCacheInMem<String>();
	}

	public Map<String, Object> consumerProps() {

		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-retry");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		return props;
	}
	
	private KafkaConsumerInfo consumerInfo(String id, String topic, Map<String, Object> consumerProps, int concurrency) {

		return new KafkaConsumerInfo(id, topic, consumerProps, concurrency);
	}

}
