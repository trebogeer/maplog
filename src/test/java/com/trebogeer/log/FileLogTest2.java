package com.trebogeer.log;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 3/19/15
 *         Time: 11:41 AM
 */
public class FileLogTest2 {

    public static void main(String... args) {
        try (FileLog fileLog = new FileLog("dude-ext-id", new FileLogConfig())) {
            fileLog.open();
            for (int ii = 0; ii < 1; ii++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 100000000; i++) {

                    fileLog.getEntry(String.format("spin_prod_5012%d?hei=624&wid=624&op_sharpen=1", (ii + 1) * i).getBytes());
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
