package com.trebogeer.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 3/19/15
 *         Time: 11:41 AM
 */
public class FileLogTest2 {

    public static void main(String... args) {
        try (FileLog fileLog = new FileLog("dude-ext-id", new FileLogConfig().withDirectory(System.getProperty("user.home") + File.separator + "tmp" + File.separator))) {
            fileLog.open();
            for (int ii = 0; ii < 1; ii++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 10000000; i++) {

                    ByteBuffer bb = fileLog.getEntry(String.format("spin_prod_5012%d?hei=624&wid=624&op_sharpen=1", (ii + 1) * i).getBytes());
                    if (bb == null) {
                        System.out.println("Entry is null for key : " + String.format("spin_prod_5012%d?hei=624&wid=624&op_sharpen=1", (ii + 1) * i));
                        continue;
                    }
                    while (bb.hasRemaining()) {
                        bb.get();
                    }
                }

                System.out.println("Elapsed time: " + (System.currentTimeMillis() - start));
            }
            ByteBuffer b = fileLog.getEntry(String.format("spin_prod_5012%d?hei=624&wid=624&op_sharpen=1", 20).getBytes());
            byte[] s = new byte[b.limit()];
            b.get(s);
            System.out.println(new String(s));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
