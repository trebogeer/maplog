package com.trebogeer.maplog.index;

import com.trebogeer.maplog.Index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dimav
 *         Date: 3/18/15
 *         Time: 10:19 AM
 */
public class ConcurrentHashMapIndex implements Index<Long> {

    private final ConcurrentHashMap<Long, Index.Value> index = new ConcurrentHashMap<>();

    @Override
    public void put(Long aLong, Value value) {
        index.put(aLong, value);
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
