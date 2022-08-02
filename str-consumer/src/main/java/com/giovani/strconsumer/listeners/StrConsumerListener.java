package com.giovani.strconsumer.listeners;

import com.giovani.strconsumer.custom.StrConsumerCustomListener;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class StrConsumerListener {

    @SneakyThrows
    @StrConsumerCustomListener(groupId = "group-1")
    public void listener1(String message) {
        log.info("CREATE => Receive message {}", message);
        throw new IllegalArgumentException("EXCEPTION...");
    }

    @StrConsumerCustomListener(groupId = "group-1")
    public void listener2(String message) {
        log.info("LOG => Receive message {}", message);
    }

    @KafkaListener(groupId = "group-2", topics = "str-topic",containerFactory = "validMessageContainerFactory")
    public void listener3(String message) {
        log.info("HISTORY => Receive message {}", message);
    }

//Old version
/*    @KafkaListener(groupId = "group-1",
            topicPartitions = {
                    @TopicPartition(topic = "str-topic", partitions = {"0"})
            },
            containerFactory = "strContainerFactory")
    public void listener1(String message) {
        log.info("Receive message {}", message);
    }

    @KafkaListener(groupId = "group-1",
            topicPartitions = {
                    @TopicPartition(topic = "str-topic", partitions = {"1"})
            },
            containerFactory = "strContainerFactory")
    public void listener2(String message) {
        log.info("Receive message {}", message);
    }

    @KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "strContainerFactory")
    public void listener3(String message) {
        log.info("Receive message {}", message);
    }*/

}
