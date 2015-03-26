package com.trebogeer.maplog.hash;

import java.util.function.Function;

/**
 * @author dimav
 *         Date: 3/26/15
 *         Time: 11:00 AM
 */
public interface Hash extends Function<byte[], Long> {

    long hash(byte[] bytes);

    default Long apply(byte[] data) {
        return hash(data);
    }

}
