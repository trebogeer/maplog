package com.trebogeer.maplog;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.trebogeer.maplog.TestUtils.key_template;
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
        InputStream fis = FileLogTest1.class.getResourceAsStream("/image");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            TestUtils.pipe(fis, baos);
        } catch (IOException e) {
            e.printStackTrace();
        }


        byte[] image = baos.toByteArray();

        int crc = Utils.src32_t(image);
        try (FileLog fileLog = new FileLog("images1", new FileLogConfig().withDirectory(path))) {
            fileLog.open();
            for (int ii = 0; ii < 1; ii++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < total_workers * work_size_per_worker; i++) {
                    String key = String.format(key_template, i);
                    ByteBuffer bb = fileLog.getEntry(key.getBytes());
                    if (bb == null) {
                        utlogger.info("Entry is null for key : " + key);
                        continue;
                    }
                    if (Utils.src32_t(bb) != crc) {
                        utlogger.info("Corrupted entry is detected for entry : " + key);
                    }
                }

                utlogger.info("Elapsed time: " + (System.currentTimeMillis() - start));
            }
//            ByteBuffer b = fileLog.getEntry(String.format("nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1", 20).getBytes());
//            byte[] s = new byte[b.limit()];
//            b.get(s);
//            System.out.println(new String(s));
        } catch (IOException e) {
            utlogger.error("IO", e.getMessage());
        }
    }
}
