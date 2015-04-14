package com.trebogeer.maplog;

/**
 * @author dimav
 *         Date: 4/6/15
 *         Time: 1:08 PM
 */
public final class Constants {

    private Constants() {
    }

    //public static final int INDEX_ENTRY_SIZE = 29;
    // TODO need to align in to 32 bytes.
    public static final int INDEX_ENTRY_SIZE =
                    8 /*64 bit id*/ +
                    8 /*position*/ +
                    4 /*offset*/ +
                    1 /*flags*/ +
                    8 /*timestamp nanos*/;
    public static final int CRC_AND_SIZE = 12;
}
