package com.trebogeer.maplog.checksum;

import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 4/7/15
 *         Time: 10:31 AM
 */
public class CRC32 implements Checksum {


    private final static ThreadLocal<java.util.zip.CRC32> crc32 = new ThreadLocal<java.util.zip.CRC32>() {

        @Override
        protected java.util.zip.CRC32 initialValue() {
            return new java.util.zip.CRC32();
        }


        @Override
        public java.util.zip.CRC32 get() {
            java.util.zip.CRC32 crc32 = super.get();
            crc32.reset();
            return crc32;
        }
    };

    /**
     * Calculates 32-bit checksum and resets byte buffer to previous state
     *
     * @param bb source byte buffer
     * @return 32-bit checksum
     */
    @Override
    public int checksum(ByteBuffer bb) {
        bb.mark();
        java.util.zip.CRC32 a32 = crc32.get();
        a32.update(bb);
        bb.reset();
        return (int) a32.getValue();
    }
}
