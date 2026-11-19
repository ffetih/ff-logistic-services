package com.siose.ff.logistics;

import com.siose.ff.base.app.FfApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The warehousing area as a service. Its business code is shared's ff-domain-warehousing, the same jar the
 * modulith runs; component scanning under {@code com.siose.ff.logistics} finds it, and with only
 * this one core on the classpath, only this area is here.
 */
@SpringBootApplication
public class WarehousingApplication {

    public static void main(String[] args) {
        FfApplication.run(WarehousingApplication.class, args);
    }
}
