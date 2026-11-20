package com.siose.ff.logistics.transportation.messaging;

import com.siose.ff.logistics.transportation.ShipmentDelivered;
import com.siose.ff.logistics.transportation.ShipmentProgressed;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/**
 * The Kafka topics the transportation service writes to: it owns them. Spring's KafkaAdmin creates
 * a missing topic when the service starts and never shrinks an existing one; the broker creates
 * none by itself. Not in the migration Job ({@code migrate}), which should not wait for a broker,
 * and only when the events are sent at all.
 *
 * <p>Only in the service: the modulith keeps its events in its JVM (S6 sends them out from both
 * sides).
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "spring.modulith.events.externalization.enabled", havingValue = "true")
@Profile("!migrate")
class ShipmentTopics {

    @Bean
    NewTopic shipmentDeliveredTopic(@Value("${ff.kafka.partitions:3}") int partitions,
            @Value("${ff.kafka.replicas:1}") int replicas) {
        return TopicBuilder.name(ShipmentDelivered.TOPIC).partitions(partitions).replicas(replicas).build();
    }

    @Bean
    NewTopic shipmentProgressedTopic(@Value("${ff.kafka.partitions:3}") int partitions,
            @Value("${ff.kafka.replicas:1}") int replicas) {
        return TopicBuilder.name(ShipmentProgressed.TOPIC).partitions(partitions).replicas(replicas).build();
    }
}
