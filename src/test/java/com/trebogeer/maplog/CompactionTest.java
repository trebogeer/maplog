package com.trebogeer.maplog;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.trebogeer.maplog.TestUtils.key_template;
import static com.trebogeer.maplog.TestUtils.segment_size;
import static com.trebogeer.maplog.TestUtils.utlogger;

/**
 * @author dimav
 *         Date: 3/27/15
 *         Time: 1:46 PM
 */
public class CompactionTest {

    public static void main(String... args) {


        String path = System.getProperty("user.home") + File.separator + /*"tmp"*/"nfsshare" + File.separator;
        InputStream fis = FileLogTest1.class.getResourceAsStream("/image");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            TestUtils.pipe(fis, baos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] image = baos.toByteArray();


        FileLogConfig cfg = new FileLogConfig().withDirectory(path)
                .withFlushOnWrite(true).withFileLocks(true).withSegmentSize(segment_size);
        try (FileLog fileLog = new FileLog("images0", cfg)) {
            fileLog.open();

            for (int a = 0; a < 2; a++) {
                for (int i = 0; i < segment_size / image.length / 2 + 100; i++) {
                    int l = image.length;
                    ByteBuffer bb = ByteBuffer.allocate(l);
                    bb.put(image);
                    bb.rewind();
                    fileLog.appendEntry(bb, String.format(key_template, i).getBytes(), (byte) 6);
                }
            }

            fileLog.firstSegment().compact();
        } catch (IOException e) {
            utlogger.error("IO", e);
        }


    }


}