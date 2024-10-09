package org.example.kafka.email.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class UnsubscribeConsumer {

    private static final Logger logger = LoggerFactory.getLogger(UnsubscribeConsumer.class);

    @KafkaListener(topics = "unsubscribe-topic", groupId = "unsubscribe-group")
    public Mono<Void> consumeUnsubscribeEvent(ConsumerRecord<String, String> record) {
        String email = record.value();
        return processUnsubscribeEvent(email)
                .doOnSuccess(success -> logger.info("Successfully processed unsubscribe for: {}", email))
                .doOnError(error -> logger.error("Failed to process unsubscribe for: {}, Error: {}", email, error.getMessage()))
                .then();
    }

    private Mono<Void> processUnsubscribeEvent(String email) {
        // Simulate processing of the email (e.g., database update)
        return Mono.fromRunnable(() -> logger.info("Processing unsubscribe request for: {}", email)).then();
    }
}
