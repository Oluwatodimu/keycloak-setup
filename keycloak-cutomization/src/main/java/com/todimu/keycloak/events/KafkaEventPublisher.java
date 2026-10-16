package com.todimu.keycloak.events;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KafkaEventPublisher implements AutoCloseable {

    private static final Logger log = Logger.getLogger(KafkaEventPublisher.class);

    private final Producer<String, String> producer;
    private final String topic;
    private final ExecutorService sender;

    public KafkaEventPublisher(Producer<String, String> producer, String topic, int queueSize) {
        this.producer = producer;
        this.topic = topic;
        this.sender = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueSize),
                runnable -> {
                    Thread thread = new Thread(runnable, "kafka-event-publisher");
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }


    public void publish(String key, String eventType, String json) {
        try {
            sender.execute(() -> send(key, eventType, json));
        } catch (RejectedExecutionException e) {
            log.warnf("Kafka event listener: queue full or shutting down, dropped %s event (key=%s)", eventType, key);
        }
    }

    private void send(String key, String eventType, String json) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, json);
            // consumers can route/filter on this header without parsing the JSON
            record.headers().add("eventType", eventType.getBytes(StandardCharsets.UTF_8));
            producer.send(record, (metadata, error) -> {
                if (error != null) {
                    log.warnf("Kafka event listener: failed to publish %s event (key=%s): %s", eventType, key, error.getMessage());
                } else if (log.isDebugEnabled()) {
                    log.debugf("Kafka event listener: published %s to %s-%d@%d",
                            eventType, metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            log.warnf("Kafka event listener: could not send %s event (key=%s): %s", eventType, key, e.getMessage());
        }
    }

    @Override
    public void close() {
        sender.shutdown();
        try {
            if (!sender.awaitTermination(5, TimeUnit.SECONDS)) {
                sender.shutdownNow();
            }
        } catch (InterruptedException e) {
            sender.shutdownNow();
            Thread.currentThread().interrupt();
        }
        producer.close(Duration.ofSeconds(5));
    }
}
