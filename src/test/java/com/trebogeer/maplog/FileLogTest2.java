package com.trebogeer.maplog;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

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
                for (int i = 0; i < /*10000000*/5000/*000*/; i++) {

                    ByteBuffer bb = fileLog.getEntry(String.format("nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1", (ii + 1) * i).getBytes());
                    if (bb == null) {
                        System.out.println("Entry is null for key : " + String.format("nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1", (ii + 1) * i));
                        continue;
                    }
                    if (Utils.src32_t(bb) != crc) {
                        System.out.println("Corrupted entry is detected for entry : " + String.format("nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1", (ii + 1) * i));
                    }
                }

                System.out.println("Elapsed time: " + (System.currentTimeMillis() - start));
            }
            ByteBuffer b = fileLog.getEntry(String.format("nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1", 20).getBytes());
            byte[] s = new byte[b.limit()];
            b.get(s);
            System.out.println(new String(s));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
