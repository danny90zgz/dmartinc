package com.dmartinc.kafka.components;

import static com.dmartinc.kafka.events.Events.EVENT_ONE;
import static com.dmartinc.kafka.events.Events.EVENT_TWO;

import java.util.UUID;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.dmartinc.kafka.events.EventOne;
import com.dmartinc.kafka.events.EventTwo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Component
@KafkaListener(id = "consumer-" + EVENT_ONE,
               topics = {EVENT_ONE})
public class KafkaConsumer {

    private final KafkaTemplate<Object, Object> template;

    @KafkaHandler
    public void handle(EventOne eventOne) {
        log.info("Received:{}", eventOne);
        template.send(EVENT_TWO, new EventTwo(eventOne.getField1(), eventOne.getField2(), UUID.randomUUID().toString()));
    }
}
