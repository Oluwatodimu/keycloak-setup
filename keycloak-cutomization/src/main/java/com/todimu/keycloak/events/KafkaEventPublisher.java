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
import java.util.function.Supplier;


public class KafkaEventPublisher implements AutoCloseable {

    private static final Logger log = Logger.getLogger(KafkaEventPublisher.class);
    private static final long RETRY_INTERVAL_MS = 30_000;

    private final Supplier<Producer<String, String>> producerFactory;
    private final String topic;
    private final ExecutorService sender;

    // only touched by the single sender thread (and close() after it has stopped)
    private Producer<String, String> producer;
    private long nextAttemptAt = 0;
    private long droppedSinceLastWarning = 0;

    public KafkaEventPublisher(Supplier<Producer<String, String>> producerFactory, String topic, int queueSize) {
        this.producerFactory = producerFactory;
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

    /** Tries to connect right away (in the background) so problems show up in the startup logs. */
    public void start() {
        try {
            sender.execute(this::producer);
        } catch (RejectedExecutionException ignored) {
            // shutting down
        }
    }

    public void publish(String key, String eventType, String json) {
        try {
            sender.execute(() -> send(key, eventType, json));
        } catch (RejectedExecutionException e) {
            log.warnf("Kafka event listener: queue full or shutting down, dropped %s event (key=%s)", eventType, key);
        }
    }

    private void send(String key, String eventType, String json) {
        Producer<String, String> p = producer();
        if (p == null) {
            droppedSinceLastWarning++;
            return; // Kafka not available yet; reported by producer()
        }
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, json);
            // consumers can route/filter on this header without parsing the JSON
            record.headers().add("eventType", eventType.getBytes(StandardCharsets.UTF_8));
            p.send(record, (metadata, error) -> {
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

    /** Returns the producer, creating it if needed; null while Kafka can't be reached (retried every 30s). */
    private Producer<String, String> producer() {
        if (producer != null) {
            return producer;
        }
        long now = System.currentTimeMillis();
        if (now < nextAttemptAt) {
            return null;
        }
        try {
            producer = producerFactory.get();
            log.info("Kafka event listener: connected producer to Kafka");
            if (droppedSinceLastWarning > 0) {
                log.warnf("Kafka event listener: %d event(s) were dropped while Kafka was unavailable", droppedSinceLastWarning);
                droppedSinceLastWarning = 0;
            }
            return producer;
        } catch (Exception e) {
            nextAttemptAt = now + RETRY_INTERVAL_MS;
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.warnf("Kafka event listener: cannot create Kafka producer (%s); events are dropped, retrying in %ds",
                    cause.getMessage(), RETRY_INTERVAL_MS / 1000);
            return null;
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
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
    }
}
