package com.trebogeer.maplog.checksum;

import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 4/7/15
 *         Time: 10:25 AM
 */
public class Adler32 implements Checksum {

    private final static ThreadLocal<java.util.zip.Adler32> adler32 = new ThreadLocal<java.util.zip.Adler32>() {

        @Override
        protected java.util.zip.Adler32 initialValue() {
            return new java.util.zip.Adler32();
        }


        @Override
        public java.util.zip.Adler32 get() {
            java.util.zip.Adler32 a32 = super.get();
            a32.reset();
            return a32;
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
        java.util.zip.Adler32 a32 = adler32.get();
        a32.update(bb);
        bb.reset();
        return (int) a32.getValue();
    }
}
