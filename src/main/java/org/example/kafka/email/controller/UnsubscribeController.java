package org.example.kafka.email.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@RequestMapping("/api/unsubscribe")
public class UnsubscribeController {

    private static final Logger logger = LoggerFactory.getLogger(UnsubscribeController.class);
    private final UnsubscribeProducer unsubscribeProducer;

    public UnsubscribeController(UnsubscribeProducer unsubscribeProducer) {
        this.unsubscribeProducer = unsubscribeProducer;
    }

    @PostMapping
    public Mono<Void> unsubscribe(@RequestBody String email) {
        logger.info("Received unsubscribe request for email: {}", email);
        return unsubscribeProducer.sendUnsubscribeEvent(email)
                .doOnSuccess(success -> logger.info("Unsubscribe request for [{}] processed", email))
                .doOnError(error -> logger.error("Error processing unsubscribe request for [{}]: {}", email, error.getMessage()));
    }
}
