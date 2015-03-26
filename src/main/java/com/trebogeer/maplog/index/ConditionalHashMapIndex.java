package com.trebogeer.maplog.index;

import com.trebogeer.maplog.Index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * @author dimav
 *         Date: 3/18/15
 *         Time: 10:19 AM
 */
public class ConditionalHashMapIndex implements Index<Long> {

    private final ConditionalConcurrentHashMap index = new ConditionalConcurrentHashMap();

    @Override
    public void put(Long aLong, Value value) {
        index.putIf(aLong, value, (value1, value2) -> {
            short s1 = value1.getSegmentId();
            short s2 = value2.getSegmentId();
            return s1 < s2 || (s1 == s2 && value1.getOffset() < value2.getOffset());
        });
    }

    @Override
    public Value get(Long aLong) {
        return index.get(aLong);
    }

    public long size() {
        return index.size();
    }

    @Override
    public void putAll(Map<Long, Value> subset) {
        index.putAll(subset);
    }
}
