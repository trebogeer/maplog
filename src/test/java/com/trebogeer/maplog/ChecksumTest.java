package com.trebogeer.maplog;

import com.trebogeer.maplog.checksum.Adler32;
import com.trebogeer.maplog.checksum.CRC32;
import com.trebogeer.maplog.checksum.Fletcher32;
import com.trebogeer.maplog.hash.MurMur3;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.nio.ByteBuffer;

/**
 * CRC32 looks better then any other checksum/hash on my
 * <p/>
 * Linux dimav 3.13.0-37-generic #64-Ubuntu SMP Mon Sep 22 21:28:38 UTC 2014 x86_64 x86_64 x86_64 GNU/Linux
 * <p/>
 * with xxhash being next closest.
 * Looks like jdk dev finally got CRC32 right. Need to check murmur 32 as well.
 * <p/>
 * CRC32 758300 0.0608
 * Adler 758300 0.6788
 * Adler Direct 758300 0.6789
 * Fletcher 758300 2.506
 * XXHash Native 758300 0.1183
 * XXHash Unsafe 758300 0.1208
 * XXHash Safe 758300 0.3617
 * CRC32 29238 0.0024
 * Adler 29238 0.0266
 * Adler Direct 29238 0.0264
 * Fletcher 29238 0.0974
 * XXHash Native 29238 0.0047
 * XXHash Unsafe 29238 0.0047
 * XXHash Safe 29238 0.0147
 *
 * @author dimav
 *         Date: 4/10/15
 *         Time: 4:29 PM
 */
public class ChecksumTest {

    public static void main(String... args) {
        long ii = 123;

        byte[][] image = {TestUtils.get1mbImage(), TestUtils.get30KBImage(), TestUtils.get30MBImage()};
        for (byte[] anImage : image) {
            ByteBuffer bb = ByteBuffer.wrap(anImage);
            bb.rewind();
            int r = 10000;
            CRC32 crc32 = new CRC32();
            long start = System.currentTimeMillis();

            for (int i = 0; i < r; i++) {
                ii %= crc32.checksum(bb);

            }
            System.out.println("CRC32 " + anImage.length + " " + (System.currentTimeMillis() - start) / (r * 1f));

            Adler32 a = new Adler32();
            start = System.currentTimeMillis();
            for (int i = 0; i < r; i++) {
                ii %= a.checksum(bb);
            }
            System.out.println("Adler " + anImage.length + " " + (System.currentTimeMillis() - start) / (r * 1f));

            ByteBuffer bbDirect = ByteBuffer.allocateDirect(anImage.length);
            bbDirect.put(anImage);
            bbDirect.rewind();
            Adler32 aa = new Adler32();
            start = System.currentTimeMillis();
            for (int i = 0; i < r; i++) {
                ii %= aa.checksum(bb);
                bbDirect.rewind();
            }
            System.out.println("Adler Direct " + anImage.length + " " + (System.currentTimeMillis() - start) / (r * 1f));

            Fletcher32 f = new Fletcher32();
            start = System.currentTimeMillis();
            for (int i = 0; i < r; i++) {
                ii %= f.checksum(bb);
            }
            System.out.println("Fletcher " + anImage.length + " " + (System.currentTimeMillis() - start) / (r * 1f));


            XXHash32 hash = XXHashFactory.nativeInstance().hash32();
            start = System.currentTimeMillis();
            for (int i = 0; i < r; i++) {
                ii %= hash.hash(bb, 127);
                bb.rewind();

            }
            System.out.println("XXHash Native " + anImage.length + " " + (System.currentTimeMillis() - start) / (r * 1f));

            hash = XXHashFactory.unsafeInstance().hash32();
            start = System.currentTimeMillis();
            for (int i = 0; i < r; i++) {
                ii %= hash.hash(bb, 127);
                bb.rewind();

            }
            System.out.println("XXHash Unsafe " + anImage.length + " " + (System.currentTimeMillis() - start) / (r * 1f));

            hash = XXHashFactory.safeInstance().hash32();
            start = System.currentTimeMillis();
            for (int i = 0; i < r; i++) {
                ii %= hash.hash(bb, 127);
                bb.rewind();

            }
            System.out.println("XXHash Safe " + anImage.length + " " + (System.currentTimeMillis() - start) / (r * 1f));

            start = System.currentTimeMillis();
            for (int i = 0; i < r; i++) {
                ii %= MurMur3.MurmurHash3_x64_32(bb.array(), 127);
                bb.rewind();

            }
            System.out.println("MurMur3 32 " + anImage.length + " " + (System.currentTimeMillis() - start) / (r * 1f));


            System.out.println("=======================================");
        }
        System.out.println(ii);

    }
}
