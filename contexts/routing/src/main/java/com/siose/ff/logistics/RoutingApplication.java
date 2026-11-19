package com.siose.ff.logistics;

import com.siose.ff.base.app.FfApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The routing area as a service. Its business code is shared's ff-domain-routing, the same jar the
 * modulith runs; component scanning under {@code com.siose.ff.logistics} finds it, and with only
 * this one core on the classpath, only this area is here.
 */
@SpringBootApplication
public class RoutingApplication {

    public static void main(String[] args) {
        FfApplication.run(RoutingApplication.class, args);
    }
}
