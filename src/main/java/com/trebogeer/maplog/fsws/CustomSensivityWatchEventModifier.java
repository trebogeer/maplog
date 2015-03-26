package com.trebogeer.maplog.fsws;

import java.nio.file.WatchEvent;

/**
 * @author dimav
 *         Date: 3/26/15
 *         Time: 12:08 PM
 */
public enum CustomSensivityWatchEventModifier implements WatchEvent.Modifier {

    NEAR_REAL_TIME(100),
    VERY_HIGH(1000),
    HIGH(2000),
    MEDIUM(10000),
    LOW(30000);

    private final int sensitivity;

    public int sensitivityValueInSeconds() {
        return this.sensitivity/1000;
    }

    public int sensitivityValueInMilliseconds() {
        return this.sensitivity;
    }

    private CustomSensivityWatchEventModifier(int var3) {
        this.sensitivity = var3;
    }


}
