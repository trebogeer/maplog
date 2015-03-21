package com.trebogeer.maplog;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 3:52 PM
 */
public class FileLogTest1 {

    public static void main(String... args) {
        String path = System.getProperty("user.home") + File.separator + "tmp" + File.separator;
        InputStream fis = FileLogTest1.class.getResourceAsStream("/image");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            TestUtils.pipe(fis, baos);
        } catch (IOException e) {
            e.printStackTrace();
        }


        byte[] image = baos.toByteArray();

        FileLogConfig cfg = new FileLogConfig().withDirectory(path).withFlushOnWrite(true).withFileLocks(true);
        try (FileLog fileLog = new FileLog("images0", cfg)) {
            fileLog.open();
            for (int ii = 0; ii < 1; ii++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 10000000; i++) {
                    String rs = "stored_asset1/stored/img/gh/00/00/00/c0/10725c0a63189f34f66c67eb8660e625.img.v1";
                    String rs1 = "nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1";
                    String s = "http://abcdefght.host.asd.s.com:8080/somesys/job/lskdfjl.lsdkfj.lsdkf.sdfk/2/cfsdfe" + i + "\n";
                    byte data[] = image;
                 //   byte data[] = s.getBytes();
                    int l = data.length;

                    int totalSize = 4 + l;
                    ByteBuffer bb = ByteBuffer.allocate(totalSize);
                    bb.putInt(l);
                    bb.put(data);

                    bb.rewind();
                    fileLog.appendEntry(bb, String.format("nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1", (ii + 1) * i).getBytes());

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
