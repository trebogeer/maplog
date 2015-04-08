package com.trebogeer.maplog.checksum;

import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 4/7/15
 *         Time: 1:42 PM
 */
public class NoChecksum implements Checksum {
    /**
     * Calculates 32-bit checksum and resets byte buffer to previous state
     *
     * @param bb source byte buffer
     * @return 32-bit checksum
     */
    @Override
    public int checksum(ByteBuffer bb) {
        return -1;
    }
}
