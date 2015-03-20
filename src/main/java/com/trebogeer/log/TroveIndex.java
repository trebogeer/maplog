package com.trebogeer.log;

import gnu.trove.map.hash.THashMap;

import java.util.Map;

/**
 * @author dimav
 *         Date: 3/19/15
 *         Time: 5:24 PM
 */
public class TroveIndex implements Index<Long> {

    THashMap<Long, Value> index = new THashMap<>();

    @Override
    public void put(Long aLong, Value v) {
        index.put(aLong, v);
    }

    @Override
    public Value get(Long aLong) {
        return index.get(aLong);
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
