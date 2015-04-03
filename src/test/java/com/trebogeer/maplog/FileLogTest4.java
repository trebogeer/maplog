package com.trebogeer.maplog;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.trebogeer.maplog.TestUtils.utlogger;

/**
 * @author dimav
 *         Date: 3/23/15
 *         Time: 3:10 PM
 */
public class FileLogTest4 {

    public static void main(String... args) {

        String path = System.getProperty("user.home") + File.separator + "nfsshare"/*"tmp4"*/ + File.separator;
        String body = "yrtd_prod_501206901?hei=624&wid=624&op_sharpen=1";
        int crc = Utils.src32_t(body.getBytes());
        ByteBuffer b = ByteBuffer.wrap(body.getBytes());
        try (FileLog fileLog = new FileLog("images1", new FileLogConfig().withDirectory(path))) {
            fileLog.open();
            for (int ii = 0; ii < 1; ii++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 10; i++) {
                    byte[] key = ("" + i).getBytes();
                    byte flags = ((byte)1 & (byte)8);
                    fileLog.appendEntry(b, key, flags);
                    b.rewind();
                    ByteBuffer bb = fileLog.getEntry(key);
                    if (bb == null) {
                        utlogger.info("Entry is null for key : " + i);
                        continue;
                    }
                    if (Utils.src32_t(bb) != crc) {
                        utlogger.info("Corrupted entry is detected for entry : " + i);
                    }

                    byte[] s = new byte[bb.limit()];
                    bb.get(s);
                    utlogger.info(new String(s));

                }

                utlogger.info("Elapsed time (ms): " + (System.currentTimeMillis() - start));
            }

        } catch (IOException e) {
            utlogger.error("IO", e);
        } finally {
            try {
                TestUtils.deleteDir(path);
            } catch (IOException e) {
                utlogger.error("IO - cleanup", e);
            }
        }
    }

    @Test
    public void test(){
        main();
    }

}
