package com.trebogeer.maplog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

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
        byte[] image = TestUtils.get1mbImage();


        FileLogConfig cfg = new FileLogConfig().withDirectory(path)
                .withFlushOnWrite(true).withFileLocks(true).withSegmentSize(segment_size);
        try (FileLog fileLog = new FileLog(TestUtils.file_log_base, cfg)) {
            fileLog.open();
            ByteBuffer bb = ByteBuffer.wrap(image);
            for (int a = 0; a < 5; a++) {
                for (int i = 0; i < segment_size / image.length / 2 + 100; i++) {
                    bb.rewind();
                    fileLog.appendEntry(bb, (TestUtils.key_template + i).getBytes(), (byte) 6);
                }
            }

          //  fileLog.compact();
        } catch (IOException e) {
            utlogger.error("IO", e);
        }
    }
}