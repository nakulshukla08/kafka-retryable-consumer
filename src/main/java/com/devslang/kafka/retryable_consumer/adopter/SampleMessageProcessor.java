package com.devslang.kafka.retryable_consumer.adopter;

import com.devslang.kafka.retryable_consumer.core.IMessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleMessageProcessor implements IMessageProcessor<String, String> {

	Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public void process(String key, String value) {

		// logic to process message and save to target
		log.info("incoming message to processor: {}", value);

		if (value != null && value.equals("error-message")) {
			throw new RuntimeException("Custom Error!");

		}
		log.info("succesfully processed - processed message and saved to DB: {}");
	}
}
