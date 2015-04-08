package com.trebogeer.maplog.checksum;

/**
 * @author dimav
 *         Date: 4/7/15
 *         Time: 1:42 PM
 */
public final class Checksums {
    private Checksums() {
    }

    public static Checksum adler32() {
        return new Adler32();
    }

    public static Checksum crc32() {
        return new CRC32();
    }

    public static Checksum fletcher32() {
        return new Fletcher32();
    }

    public static Checksum noChecksum() {
        return new NoChecksum();
    }
}
