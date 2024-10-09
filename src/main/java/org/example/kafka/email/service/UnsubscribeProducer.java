package org.example.kafka.email.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class UnsubscribeProducer {

    private static final Logger logger = LoggerFactory.getLogger(UnsubscribeProducer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "unsubscribe-topic";

    public UnsubscribeProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTemplate.executeInTransaction(t -> true); // Initialize transactional context
    }

    public Mono<Void> sendUnsubscribeEvent(String email) {
        return Mono.fromRunnable(() -> {
            try {
                kafkaTemplate.executeInTransaction(operations -> {
                    operations.send(TOPIC, email);
                    logger.info("Unsubscribe event for email [{}] sent successfully", email);
                    return null;
                });
            } catch (Exception e) {
                logger.error("Error sending unsubscribe event for email [{}]: {}", email, e.getMessage());
                throw new RuntimeException("Failed to send unsubscribe event");
            }
        }).then();
    }
}
