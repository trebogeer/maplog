package com.trebogeer.maplog.checksum;

import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 4/7/15
 *         Time: 10:23 AM
 */
public interface Checksum {


    /**
     * Calculates 32-bit checksum and resets byte buffer to previous state
     * @param bb source byte buffer
     * @return  32-bit checksum
     */
    int checksum(ByteBuffer bb);
}
