package com.trebogeer.maplog;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.trebogeer.maplog.TestUtils.key_template;
import static com.trebogeer.maplog.TestUtils.segment_size;
import static com.trebogeer.maplog.TestUtils.test_image;
import static com.trebogeer.maplog.TestUtils.total_workers;
import static com.trebogeer.maplog.TestUtils.utlogger;
import static com.trebogeer.maplog.TestUtils.work_size_per_worker;

/**
 * @author dimav
 *         Date: 3/20/15
 *         Time: 4:19 PM
 */

// Testing multithreaded writes
public class FileLogTest3 {

    public static void main(String... args) {

        int t_w = total_workers;
        int chunk_size = work_size_per_worker;
        if (args != null && args.length > 0) {
            t_w = Integer.valueOf(args[0]);
            chunk_size = Integer.valueOf(args[1]);
        }

        ExecutorService es = Executors.newFixedThreadPool(total_workers);

        String path = System.getProperty("user.home") + File.separator + /*"tmp"*/"nfsshare" + File.separator;
        InputStream fis = FileLogTest1.class.getResourceAsStream(test_image);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            TestUtils.pipe(fis, baos);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] image = baos.toByteArray();


        FileLogConfig cfg = new FileLogConfig().withDirectory(path)
                .withFlushOnWrite(true).withFileLocks(true).withSegmentSize(segment_size);
        try (FileLog fileLog = new FileLog(TestUtils.file_log_base, cfg)) {
            fileLog.open();
            CountDownLatch latch = new CountDownLatch(t_w);
            for (int ii = 0; ii < t_w; ii++) {
                final int chunk = chunk_size;
                final int a = ii;
                es.execute(() -> {
                    long start = System.currentTimeMillis();
                    ByteBuffer bb = ByteBuffer.wrap(image);
                    for (int i = a * chunk; i < (a * chunk) + chunk; i++) {
                        bb.rewind();
                        try {
                            fileLog.appendEntry(bb, (key_template + i).getBytes(), (byte) 6);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                    latch.countDown();
                    utlogger.info("Elapsed time: " + (System.currentTimeMillis() - start));
                });

            }

            try {
                latch.await(24, TimeUnit.HOURS);
                es.shutdown();
                es.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            utlogger.error("IO", e);
        }
    }
}
