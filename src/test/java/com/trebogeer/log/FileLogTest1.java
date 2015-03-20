package com.trebogeer.log;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.trebogeer.log.Utils.toByteArray;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 3:52 PM
 */
public class FileLogTest1 {

    public static void main(String... args) {
        String path = System.getProperty("user.home") + File.separator + "tmp";
        InputStream fis = FileLogTest1.class.getResourceAsStream("/image");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            TestUtils.pipe(fis, baos);
        } catch (IOException e) {
            e.printStackTrace();
        }


        byte[] image = baos.toByteArray();


        try (FileLog fileLog = new FileLog("dude-ext-id", new FileLogConfig())) {
            fileLog.open();
            for (int ii = 0; ii < 1; ii++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 100000000; i++) {
                    String rs = "proxy_asset1/proxy/img/mp/00/00/00/c0/10725c0a63189f34f66c67eb8660e625.img.v1";
                    String rs1 = "spin_prod_501206901?hei=624&wid=624&op_sharpen=1";
                    String s = "http://sprel401p.prod.ch4.s.com:8080/jenkins/job/greenapi305p.prod.ch3.s.com/2/console" + i + "\n";
                 //   byte data[] = image;
                    byte data[] = s.getBytes();
                    int l = data.length;

                    int totalSize = 4 + l;
                    ByteBuffer bb = ByteBuffer.allocate(totalSize);
                    bb.putInt(l);
                    bb.put(data);

                    bb.rewind();
                    fileLog.appendEntry(bb, String.format("spin_prod_5012%d?hei=624&wid=624&op_sharpen=1", (ii + 1) * i).getBytes());

                }

                System.out.println("Elapsed time: " + (System.currentTimeMillis() - start));
            }
            ByteBuffer b = fileLog.getEntry(toByteArray(20));
            byte[] s = new byte[b.limit()];
            b.get(s);
            System.out.println(new String(s));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

}
