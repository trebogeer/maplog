package com.trebogeer.maplog.checksum;

import com.trebogeer.maplog.Utils;

import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 4/7/15
 *         Time: 10:33 AM
 */
public class Fletcher32 implements Checksum, java.util.zip.Checksum {


    private final static ThreadLocal<Fletcher32> f32 = new ThreadLocal<Fletcher32>() {

        @Override
        protected Fletcher32 initialValue() {
            return new Fletcher32();
        }


        @Override
        public Fletcher32 get() {
            Fletcher32 f32 = super.get();
            f32.reset();
            return f32;
        }
    };

    private int sum1 = 0;
    private int sum2 = 0;

    /**
     * Creates a new Fletcher-32 object.
     */
    public Fletcher32() {
    }

    /**
     * Returns Fletcher-32 value.
     *
     * @since Commons Checksum 1.0
     */
    public long getValue() {
        return (sum2 << 16) | sum1;
    }

    /**
     * Resets Fletcher-32 to initial value.
     *
     * @since Commons Checksum 1.0
     */
    public void reset() {
        sum1 = 0;
        sum2 = 0;
    }

    /**
     * Updates checksum with specified array of bytes.
     *
     * @param b the array of bytes to update the checksum with
     * @since Commons Checksum 1.0
     */
    public void update(byte[] b) {
        update(b, 0, b.length);
    }

    /**
     * Updates Fletcher-32 with specified array of bytes.
     *
     * @since Commons Checksum 1.0
     */
    public void update(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }
        updateBytes(b, off, len);
    }

    /**
     * Updates Fletcher-32 with specified byte.
     *
     * @since Commons Checksum 1.0
     */
    public void update(int b) {
        update(Utils.toBytes(b));
    }

    /**
     * Updates Fletcher-32 with specified array of bytes.
     *
     * @since Commons Checksum 1.0
     */
    private void updateBytes(byte[] b, int off, int len) {
        for (int i = off; i < len; i++) {
            sum1 = (sum1 + (b[i] & 0xff)) % 65535;
            sum2 = (sum2 + sum1) % 65535;
        }
    }


    /**
     * Calculates 32-bit checksum and resets byte buffer to previous state
     *
     * @param bb source byte buffer
     * @return 32-bit checksum
     */
    @Override
    public int checksum(ByteBuffer bb) {
        if (bb == null)
            throw new NullPointerException("ByteBuffer is null in Fletcher32#checksum");
        Fletcher32 f = f32.get();
        f.update(bb.array());
        return (int) f.getValue();
    }
}
