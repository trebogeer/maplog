package com.trebogeer.log;

import java.io.Serializable;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 11:45 AM
 */
public interface Segment extends Loggable, Serializable {

    /**
     * Returns the parent log.
     *
     * @return The parent log.
     */
    Log log();

    /**
     * Returns the segment id.
     *
     * @return the segment id.
     */
    long id();

    /**
     * Returns the segment timestamp.
     *
     * @return The segment timestamp.
     */
    long timestamp();


}
