package com.trebogeer.maplog.index;

import com.trebogeer.maplog.Index;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dimav
 *         Date: 3/26/15
 *         Time: 3:26 PM
 */
public class ConditionalMapTest {

    public static void main(String... args){
        ConcurrentHashMap c = new ConcurrentHashMap();
        ConditionalHashMapIndex cchm = new ConditionalHashMapIndex();

        cchm.put(1L, new Index.Value(0l, 0, (short) 0, (byte) 0));

        assert cchm.get(1l) != null;

        cchm.put(2L, new Index.Value(1l, 4, (short) 1, (byte) 0));

        assert cchm.size() == 2;

        cchm.put(2L, new Index.Value(0l, 0, (short) 2, (byte) 0));

        assert cchm.size() == 2;

        assert cchm.get(2l).getSegmentId() == (short)2;

        cchm.put(2L, new Index.Value(1l, 0, (short) 2, (byte) 0));

        assert cchm.get(2l).getSegmentId() == (short)2;
        assert cchm.get(2l).getPosition() == 1l;


    }
}
