package com.siose.ff.logistics.transportation.distances;

import com.siose.ff.logistics.transportation.DistanceLookup;
import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Flow 1 between the services: transportation's question about a road leg's distance, sent to the
 * routing service over HTTP, inside the cluster ({@code http://routing:8084}, not through the
 * gateway).
 *
 * <ul>
 * <li><b>Whose token.</b> The planner's own, passed on: routing checks it like any other request,
 *     and sees who asked. No service account, no second identity to manage.</li>
 * <li><b>How long.</b> At most {@code connect-timeout} to reach routing and {@code read-timeout}
 *     for its answer: the planner's request waits that long at worst, then the leg gets the
 *     straight-line estimate (the area decides that, not this class).</li>
 * <li><b>What fails.</b> Anything: no answer, a timeout, a 4xx or 5xx. It throws, and the area
 *     falls back. A shipment is never refused because routing is away.</li>
 * </ul>
 *
 * <p>The modulith answers the same question with a method call into routing.
 */
@Component
class RoutingDistances implements DistanceLookup {

    private final RestClient routing;

    RoutingDistances(@Value("${ff.routing-client.base-url}") String baseUrl,
            @Value("${ff.routing-client.connect-timeout:PT1S}") Duration connectTimeout,
            @Value("${ff.routing-client.read-timeout:PT2S}") Duration readTimeout) {
        JdkClientHttpRequestFactory requests = new JdkClientHttpRequestFactory(
                HttpClient.newBuilder().connectTimeout(connectTimeout).build());
        requests.setReadTimeout(readTimeout);
        this.routing = RestClient.builder().baseUrl(baseUrl).requestFactory(requests).build();
    }

    record Answer(double distanceKm) {
    }

    @Override
    public double roadKm(double fromLatitude, double fromLongitude, double toLatitude, double toLongitude) {
        Answer answer = routing.get()
                .uri(uri -> uri.path("/api/v1/geo/distance")
                        .queryParam("fromLatitude", fromLatitude).queryParam("fromLongitude", fromLongitude)
                        .queryParam("toLatitude", toLatitude).queryParam("toLongitude", toLongitude)
                        .build())
                .headers(headers -> callersToken(headers))
                .retrieve()
                .body(Answer.class);
        if (answer == null) {
            throw new IllegalStateException("Routing answered with no body");
        }
        return answer.distanceKm();
    }

    private static void callersToken(HttpHeaders headers) {
        Authentication caller = SecurityContextHolder.getContext().getAuthentication();
        if (caller instanceof JwtAuthenticationToken jwt) {
            headers.setBearerAuth(jwt.getToken().getTokenValue());
        }
    }
}
