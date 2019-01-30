package com.dmartinc.kafka.components;

import static com.dmartinc.kafka.events.Events.EVENT_TWO;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.dmartinc.kafka.events.EventTwo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Component
@KafkaListener(id = "final-consumer-" + EVENT_TWO,
               topics = {EVENT_TWO})

public class KafkaFinalConsumer {
    @KafkaHandler
    public void handle(EventTwo eventTwo) {
        log.info("Received:{}", eventTwo);
    }
}
