package com.giovani.strproducer.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Log4j2
public class StringProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {
        kafkaTemplate.send("str-topic", message).addCallback(
                sucess -> {
                    if (sucess != null) {
                        log.info("Send message with sucess");
                        log.info("Partition {}, Topic {}", sucess.getProducerRecord().partition(), sucess.getProducerRecord().topic());
                    }
                },
                error -> log.error("Error send message")
        );
    }

}
