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

/**
 * @author dimav
 *         Date: 3/20/15
 *         Time: 4:19 PM
 */

// Testing multithreaded writes
public class FileLogTest3 {

    private static int total_workers = 2;
    private static int work_size_per_worker = 2500;

    public static void main(String... args) {
        ExecutorService es = Executors.newFixedThreadPool(5);

        String path = System.getProperty("user.home") + File.separator + /*"tmp"*/"nfsshare" + File.separator;
        InputStream fis = FileLogTest1.class.getResourceAsStream("/image");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            TestUtils.pipe(fis, baos);
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileWatcher fw = new FileWatcher(null, new File(path).toPath());
        es.execute(fw);
        byte[] image = baos.toByteArray();


        FileLogConfig cfg = new FileLogConfig().withDirectory(path).withFlushOnWrite(true).withFileLocks(true);
        try (FileLog fileLog = new FileLog("images1", cfg)) {
            fileLog.open();
            CountDownLatch latch = new CountDownLatch(total_workers);
            for (int ii = 0; ii < total_workers; ii++) {

                final int a = ii;
                es.execute(() -> {
                    long start = System.currentTimeMillis();
                    int chunk = work_size_per_worker;
                    for (int i = a * chunk; i < (a * chunk) + chunk; i++) {
                        String rs = "stored_asset1/stored/img/gh/00/00/00/c0/10725c0a63189f34f66c67eb8660e625.img.v1";
                        String rs1 = "nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1";
                        String s = "http://abcdefght.host.asd.s.com:8080/somesys/job/lskdfjl.lsdkfj.lsdkf.sdfk/2/cfsdfe" + i + "\n";
                        byte data[] = image;
                        //   byte data[] = s.getBytes();
                        int l = data.length;

                       // int totalSize = 4 + l;
                        ByteBuffer bb = ByteBuffer.allocate(l);
                        //bb.putInt(l);
                        bb.put(data);

                        bb.rewind();
                        try {
                            fileLog.appendEntry(bb, String.format("nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1", i).getBytes());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                    latch.countDown();
                    System.out.println("Elapsed time: " + (System.currentTimeMillis() - start));
                });

            }

            try {
                latch.await(24, TimeUnit.HOURS);
                fw.shutdown();
                es.shutdown();
                es.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
