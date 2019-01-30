package com.dmartinc.kafka.components;

import static com.dmartinc.kafka.events.Events.EVENT_ONE;

import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.dmartinc.kafka.events.EventOne;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@EnableScheduling
@Component
public class KafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedRate = 5_000L)
    public void produce() {
        log.info("producing....");
        EventOne eventOne = new EventOne(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(EVENT_ONE, eventOne);
        future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                log.info("Sent message=[" + eventOne + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                log.info("Unable to send message=[" + eventOne + "] due to : " + ex.getMessage());
            }
        });
    }
}
