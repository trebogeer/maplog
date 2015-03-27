package com.trebogeer.maplog;

import com.trebogeer.maplog.fsws.FileWatcher;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.trebogeer.maplog.TestUtils.utlogger;

/**
 * @author dimav
 *         Date: 3/25/15
 *         Time: 4:01 PM
 */
public class WatchDirTest {

    public static void main(String... args) {
        try {

            ExecutorService es = Executors.newFixedThreadPool(1);

            String path = System.getProperty("user.home") + File.separator + /*"tmp"*/"nfsshare" + File.separator;
            FileWatcher fw = new FileWatcher(null, new File(path).toPath(), false);
            es.execute(fw);
            es.shutdown();
            es.awaitTermination(15, TimeUnit.MINUTES);
            es.shutdownNow();
        } catch (Exception e) {
            utlogger.error("E", e);
        }

    }
}
