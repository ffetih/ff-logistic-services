package com.siose.ff.logistics.transportation.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

/**
 * What happens to a message the consumer cannot handle.
 *
 * <ul>
 *   <li>Tried again twice, a second apart: a passing problem, such as the database restarting,
 *       is over by then.</li>
 *   <li>Then, or at once for a message that is not valid JSON or misses its keys (trying again
 *       cannot help: a message the JSON converter cannot read counts as such too), it goes to this consumer's dead-letter topic,
 *       {@code <topic>.transportation.dlt}, with the error in its headers, and the consumer moves
 *       on: one bad message does not stop the stops behind it.</li>
 * </ul>
 *
 * The dead letter is written as it was read, as bytes, with this service's own producer.
 *
 * The dead-letter topic is the transportation service's own: another service reading the same topic
 * has one of its own.
 */
@Configuration(proxyBeanMethods = false)
class KafkaErrorHandling {

    static final String DEAD_LETTER_SUFFIX = ".transportation.dlt";

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> kafka) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, exception) -> new TopicPartition(record.topic() + DEAD_LETTER_SUFFIX, -1));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000, 2));
        handler.addNotRetryableExceptions(JacksonException.class, IllegalArgumentException.class);
        return handler;
    }

    @Bean
    @Profile("!migrate")
    NewTopic completedStopsDeadLetterTopic(@Value("${ff.kafka.partitions:3}") int partitions,
            @Value("${ff.kafka.replicas:1}") int replicas) {
        return TopicBuilder.name(CompletedStops.TOPIC + DEAD_LETTER_SUFFIX).partitions(partitions)
                .replicas(replicas).build();
    }
}
