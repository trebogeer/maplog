package com.trebogeer.log;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * @author <a href="http://github.com/trebogeer">Dmitry Vasilyev</a>
 */
public class MapDB {
    
    public static void main(String... args) {
        DB db = DBMaker.newFileDB(new File(System.getProperty("user.home") + File.separator + "tmp" + File.separator)).make();

        ConcurrentNavigableMap<com.trebogeer.log.DictZip.ByteArrayWrapper, Long> treeMap = db.getTreeMap("map");
        treeMap.put(new DictZip.ByteArrayWrapper("key".getBytes()), 1L);

        db.commit();
        db.close();
    }
}
