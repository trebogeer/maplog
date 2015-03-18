package com.trebogeer.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 3:52 PM
 */
public class FileLogTest1 {

    public static void main(String... args) {
        String path = System.getProperty("user.home") + File.separator + "tmp";
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }


        try (FileLog fileLog = new FileLog("dude-ext-id", new FileLogConfig())) {
            fileLog.open();
            for (int ii = 0; ii < 1; ii++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 300/*000000*/; i++) {
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
                    fileLog.appendEntry(bb, longToBytes((ii + 1) * i));

                }

                System.out.println("Elapsed time: " + (System.currentTimeMillis() - start));
            }
            ByteBuffer b = fileLog.getEntry(longToBytes(20));
            byte[] s = new byte[b.limit()];
            b.get(s);
            System.out.println(new String(s));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


    public static byte[] longToBytes(long l) {
        ArrayList<Byte> bytes = new ArrayList<Byte>();
        while (l != 0) {
            bytes.add((byte) (l % (0xff + 1)));
            l = l >> 8;
        }
        byte[] bytesp = new byte[bytes.size()];
        for (int i = bytes.size() - 1, j = 0; i >= 0; i--, j++) {
            bytesp[j] = bytes.get(i);
        }
        return bytesp;
    }

}
