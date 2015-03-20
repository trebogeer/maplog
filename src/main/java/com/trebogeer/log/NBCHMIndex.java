package com.trebogeer.log;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.util.Map;

/**
 * @author dimav
 *         Date: 3/18/15
 *         Time: 4:54 PM
 */
// a lot slower compared to concurrent hash map
public class NBCHMIndex implements Index<Long> {

    // optimizing for space
    NonBlockingHashMapLong<Value> index = new NonBlockingHashMapLong<>(true);
    @Override
    public void put(Long l, Value v) {
         index.put(l, v);
    }

    @Override
    public Value get(Long l) {
        return index.get(l);
    }

    @Override
    public long size() {
        return index.size();
    }

    @Override
    public void putAll(Map<Long, Value> subset) {
        index.putAll(subset);
    }
}
