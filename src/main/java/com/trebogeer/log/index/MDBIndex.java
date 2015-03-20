package com.trebogeer.log.index;

import com.trebogeer.log.Index;
//import org.mapdb.DB;
//import org.mapdb.DBMaker;

import java.util.Map;

/**
 * @author dimav
 *         Date: 3/19/15
 *         Time: 4:07 PM
 */

// no so fast - actually very slow.
public class MDBIndex implements Index<Long> {
//
//    private final DB db = DBMaker.newMemoryDirectDB().cacheSize(1).make();
//    private Map<Long, Value> index = db.createTreeMap("index").valuesOutsideNodesEnable().nodeSize(6).make();

    @Override
    public void put(Long aLong, Value v) {
//        index.put(aLong, v);
    }

    @Override
    public Value get(Long aLong) {
        return null;//index.get(aLong);
    }

    @Override
    public long size() {
        return 0;//index.size();
    }

    @Override
    public void putAll(Map<Long, Value> subset) {
      //  index.putAll(subset);
    }
}
