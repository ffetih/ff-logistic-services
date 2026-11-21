package com.siose.ff.logistics.routing.messaging;

import com.siose.ff.logistics.routing.RouteStopCompleted;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/**
 * The Kafka topics the routing service writes to: it owns them. Spring's KafkaAdmin creates
 * a missing topic when the service starts and never shrinks an existing one; the broker creates
 * none by itself. Not in the migration Job ({@code migrate}), which should not wait for a broker,
 * and only when the events are sent at all.
 *
 * <p>The modulith creates the same topics with the same partitions (its {@code EventTopics}, S6):
 * whichever side runs first creates a topic, and the other finds it.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "spring.modulith.events.externalization.enabled", havingValue = "true")
@Profile("!migrate")
class RouteStopTopics {

    @Bean
    NewTopic routeStopCompletedTopic(@Value("${ff.kafka.partitions:3}") int partitions,
            @Value("${ff.kafka.replicas:1}") int replicas) {
        return TopicBuilder.name(RouteStopCompleted.TOPIC).partitions(partitions).replicas(replicas).build();
    }
}
