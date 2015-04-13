package com.trebogeer.maplog;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.trebogeer.maplog.TestUtils.key_template;
import static com.trebogeer.maplog.TestUtils.segment_size;
import static com.trebogeer.maplog.TestUtils.total_workers;
import static com.trebogeer.maplog.TestUtils.utlogger;
import static com.trebogeer.maplog.TestUtils.work_size_per_worker;

/**
 * Batched concurrent write test
 *
 * @author dimav
 *         Date: 4/2/15
 *         Time: 12:54 PM
 */
public class FileLogTest5 {
    public static void main(String... args) {

        int t_w = total_workers;
        int chunk_size = work_size_per_worker;
        if (args != null && args.length > 0) {
            t_w = Integer.valueOf(args[0]);
            chunk_size = Integer.valueOf(args[1]);
        }

        ExecutorService es = Executors.newFixedThreadPool(total_workers);

        String path = System.getProperty("user.home") + File.separator + /*"tmp"*/"nfsshare" + File.separator;
        InputStream fis = FileLogTest1.class.getResourceAsStream("/48060-high-res-ship.jpg");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            TestUtils.pipe(fis, baos);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] image = baos.toByteArray();


        FileLogConfig cfg = new FileLogConfig().withDirectory(path)
                .withFlushOnWrite(true).withFileLocks(true).withSegmentSize(segment_size);
        try (FileLog fileLog = new FileLog("images1", cfg)) {
            fileLog.open();
            CountDownLatch latch = new CountDownLatch(t_w);
            for (int ii = 0; ii < t_w; ii++) {
                final int chunk = chunk_size;
                final int a = ii;
                es.execute(() -> {
                    long start = System.currentTimeMillis();
                    Map<byte[], Loggable.Entry> entryMap = new HashMap<>();
                    for (int i = a * chunk; i < (a * chunk) + chunk; i++) {

                        byte data[] = image;
                        //   byte data[] = s.getBytes();
                        int l = data.length;

                        // int totalSize = 4 + l;
                        ByteBuffer bb = ByteBuffer.allocate(l);
                        //bb.putInt(l);
                        bb.put(data);

                        bb.rewind();
                        entryMap.put((key_template + i).getBytes(), new Loggable.Entry(bb, (byte) 7));
                        if (entryMap.size() == 25) {

                            try {
                                fileLog.appendEntries(entryMap);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            entryMap.clear();
                        }

                    }
                    if (!entryMap.isEmpty()) {
                        try {
                            fileLog.appendEntries(entryMap);
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
