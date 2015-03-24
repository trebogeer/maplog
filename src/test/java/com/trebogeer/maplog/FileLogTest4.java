package com.trebogeer.maplog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 3/23/15
 *         Time: 3:10 PM
 */
public class FileLogTest4 {

    public static void main(String... args) {

        String path = System.getProperty("user.home") + File.separator + /*"nfsshare"*/"tmp4" + File.separator;
        String body = "yrtd_prod_501206901?hei=624&wid=624&op_sharpen=1";
        int crc = Utils.src32_t(body.getBytes());
        ByteBuffer b = ByteBuffer.wrap(body.getBytes());
        try (FileLog fileLog = new FileLog("images1", new FileLogConfig().withDirectory(path))) {
            fileLog.open();
            for (int ii = 0; ii < 1; ii++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 10; i++) {
                    byte[] key = ("" + i).getBytes();
                    fileLog.appendEntry(b, key);
                    b.rewind();
                    ByteBuffer bb = fileLog.getEntry(key);
                    if (bb == null) {
                        System.out.println("Entry is null for key : " + i);
                        continue;
                    }
                    if (Utils.src32_t(bb) != crc) {
                        System.out.println("Corrupted entry is detected for entry : " + i);
                    }

                    byte[] s = new byte[bb.limit()];
                    bb.get(s);
                    System.out.println(new String(s));

                }

                System.out.println("Elapsed time: " + (System.currentTimeMillis() - start));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            new File(path).delete();
        }
    }

}
