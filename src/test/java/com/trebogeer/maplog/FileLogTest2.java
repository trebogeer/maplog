package com.trebogeer.maplog;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.trebogeer.maplog.TestUtils.file_log_base;
import static com.trebogeer.maplog.TestUtils.key_template;
import static com.trebogeer.maplog.TestUtils.test_image;
import static com.trebogeer.maplog.TestUtils.total_workers;
import static com.trebogeer.maplog.TestUtils.utlogger;
import static com.trebogeer.maplog.TestUtils.work_size_per_worker;

/**
 * @author dimav
 *         Date: 3/19/15
 *         Time: 11:41 AM
 */
public class FileLogTest2 {

    public static void main(String... args) {

        String path = System.getProperty("user.home") + File.separator + "nfsshare"/*tmp*/ + File.separator;
        InputStream fis = FileLogTest1.class.getResourceAsStream(test_image);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            TestUtils.pipe(fis, baos);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);


        byte[] image = baos.toByteArray();

        int crc = Utils.crc32_t(image);
        try (FileLog fileLog = new FileLog(file_log_base, new FileLogConfig().withDirectory(path))) {
            fileLog.open();
            int tasks = 100;
            final CountDownLatch countDownLatch = new CountDownLatch(tasks);
            for (int ii = 0; ii < tasks; ii++) {
                es.submit(() -> {
                    long start = System.currentTimeMillis();
                    for (int i = 0; i < total_workers * work_size_per_worker; i++) {
                        String key = key_template + i;
                        ByteBuffer bb = null;
                        try {
                            bb = fileLog.getEntry(key.getBytes());
                        } catch (IOException e) {
                            utlogger.error("IO", e);
                        }
                        if (bb == null) {
                            utlogger.info("Entry is null for key : " + key);
                            continue;
                        }
                        if (Utils.crc32_t(bb) != crc) {
                            utlogger.info("Corrupted entry is detected for entry : " + key);
                        }
                    }
                    countDownLatch.countDown();
                    utlogger.info("Elapsed time: " + (System.currentTimeMillis() - start));
                });
            }
            countDownLatch.await();
//            ByteBuffer b = fileLog.getEntry(String.format("nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1", 20).getBytes());
//            byte[] s = new byte[b.limit()];
//            b.get(s);
//            System.out.println(new String(s));
            es.shutdownNow();
        } catch (IOException e) {
            utlogger.error("IO", e.getMessage());
        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
        }
    }
}
