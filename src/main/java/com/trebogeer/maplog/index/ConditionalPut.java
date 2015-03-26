package com.trebogeer.maplog.index;

import java.util.function.BiFunction;

/**
 * @author dimav
 *         Date: 3/26/15
 *         Time: 3:14 PM
 */
public interface ConditionalPut<K, V> {

    V putIf(K k, V v, BiFunction<V, V, Boolean> predicate);


}
