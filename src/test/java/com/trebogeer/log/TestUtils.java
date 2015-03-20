package com.trebogeer.log;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author dimav
 *         Date: 3/18/15
 *         Time: 12:03 PM
 */
public final class TestUtils {

    public static final int BUFFER = 0x2000;

    private TestUtils() {
    }

    public static void pipe(final InputStream source, final OutputStream target) throws IOException {
        byte[] buf = new byte[BUFFER];
        while (true) {
            int r = source.read(buf);
            if (r == -1) {
                break;
            }
            target.write(buf, 0, r);
            target.flush();
        }
    }

}
