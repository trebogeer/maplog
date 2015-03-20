package com.trebogeer.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 1:12 PM
 */
public class FileLogTest0 {

    public static void main(String... args) {
        String path = System.getProperty("user.home") + File.separator + "tmp" + File.separator;
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }


        try (FileLog fileLog = new FileLog("dude", new FileLogConfig().withDirectory(path))) {
            fileLog.open();
            for (int ii = 0; ii < 1; ii++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 300; i++) {
                    String rs = "proxy_asset1/proxy/img/mp/00/00/00/c0/10725c0a63189f34f66c67eb8660e625.img.v1";
                    String rs1 = "spin_prod_501206901?hei=624&wid=624&op_sharpen=1";
                    String s = "http://sprel401p.prod.ch4.s.com:8080/jenkins/job/greenapi305p.prod.ch3.s.com/2/console" + i + "\n";
                    byte data[] = s.getBytes();
                    int l = data.length;

                    int totalSize = 4 + l;
                    ByteBuffer bb = ByteBuffer.allocate(totalSize);
                    bb.putInt(l);
                    bb.put(data);

                    bb.rewind();
                    //fileLog.appendEntry(bb);

                }

                System.out.println("Elapsed time: " + (System.currentTimeMillis() - start));
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
