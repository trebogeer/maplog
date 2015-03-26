package com.trebogeer.maplog;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.trebogeer.maplog.TestUtils.total_workers;
import static com.trebogeer.maplog.TestUtils.utlogger;
import static com.trebogeer.maplog.TestUtils.work_size_per_worker;

/**
 * @author dimav
 *         Date: 3/25/15
 *         Time: 4:01 PM
 */
public class WatchDirTest {

    public static void main(String... args) {
        try {
            int t_w = total_workers;
            int chunk_size = work_size_per_worker;
            if (args != null && args.length > 0) {
                t_w = Integer.valueOf(args[0]);
                chunk_size = Integer.valueOf(args[1]);
            }

            ExecutorService es = Executors.newFixedThreadPool(1);

            String path = System.getProperty("user.home") + File.separator + /*"tmp"*/"nfsshare" + File.separator;
            InputStream fis = FileLogTest1.class.getResourceAsStream("/image");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                TestUtils.pipe(fis, baos);
            } catch (IOException e) {
                e.printStackTrace();
            }

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
