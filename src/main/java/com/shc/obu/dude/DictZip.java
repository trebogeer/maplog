package com.shc.obu.dude;

/**
 * @author dimav
 *         Date: 3/13/15
 *         Time: 11:15 AM
 */

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

class Chunk {
    public int offset;
    public int size;

    public Chunk(int o, int s) {
        offset = o;
        size = s;
    }
}

public class DictZip {

    /**
     *
     */
    private RandomAccessFile dictzip;

    final int FTEXT = 1;
    final int FHCRC = 2;
    final int FEXTRA = 4;
    final int FNAME = 8;
    final int FCOMMENT = 16;

    final int READ = 1;
    final int WRITE = 2;

    private int pos = 0;
    private int chlen = 0;
    private int _firstpos = 0;

    public String last_error = "";

    private List<Chunk> chunks;

    /**
     * @param dictzipfilename
     */
    public DictZip(String dictzipfilename) {
        try {
            dictzip = new RandomAccessFile(dictzipfilename, "r");
            pos = 0;
            _firstpos = 0;
            chunks = new ArrayList<Chunk>();
            this._read_gzip_header();
        } catch (Exception e) {
            last_error = e.toString();
            e.printStackTrace();
        }
    }

    /**
     * @param buff
     * @param size
     * @return
     */
    public int read(byte[] buff, int size) throws Exception {
        if (size <= 0) {
            return 0;
        }
        int firstchunk = this.pos / this.chlen;
        int offset = this.pos - firstchunk * this.chlen;
        int lastchunk = (this.pos + size) / this.chlen;
        /*
         * int finish = 0;
         * int npos = 0;
         * finish = offset+size;
         * npos = this.pos+size;
         */
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        for (int i = firstchunk; i <= lastchunk; i++) {
            byteStream.write(this._readchunk(i));
        }
        byte[] buf = byteStream.toByteArray();
        for (int i = 0; i < size; i++) {
            buff[i] = buf[offset + i];
        }
        return 0;
    }

    /**
     * @param pos
     * @param where
     */
    public void seek(int pos, int where) {
        if (where == 0) {
            this.pos = pos;
        } else if (where == 1) {
            this.pos += pos;
        } else {
            this.pos = pos;
        }
    }

    /**
     * @param pos
     */
    public void seek(int pos) {
        this.seek(pos, 0);
    }

    /**
     * @return
     */
    public int tell() {
        return this.pos;
    }

    /**
     * @throws Exception
     */
    public void close() throws Exception {
        this.dictzip.close();
    }

    /**
     * @throws Exception
     */
    private void _read_gzip_header() throws Exception {
        byte[] buffer = new byte[2];
        dictzip.read(buffer);
        this._firstpos += 2;
        if (buffer[0] != 31 || buffer[1] != -117) {
            throw new Exception("Not a gzipped file");
        }
        byte b = dictzip.readByte();
        this._firstpos += 1;
        if (b != 8) {
            throw new Exception("Unknown compression method");
        }
        byte flag = dictzip.readByte();
        //System.out.println("flag = "+flag);
        this._firstpos += 1;
        dictzip.readInt();
        dictzip.readByte();
        dictzip.readByte();
        this._firstpos += 6;
        int xlen = 0;
        if ((flag & FEXTRA) != 0) {
            xlen = dictzip.readUnsignedByte();
            xlen += 256 * dictzip.readUnsignedByte();
            byte[] extra = new byte[xlen];
            dictzip.read(extra);
            this._firstpos += 2 + xlen;
            int ext = 0;
            while (true) {
                int l = ((int) extra[ext + 2] & 0xff) + (256 * ((int) extra[ext + 3] & 0xff));
                if (extra[ext + 0] != 'R' || extra[ext + 1] != 'A') {
                    ext = 4 + l;
                    if (ext > xlen) {
                        throw new Exception("Missing dictzip extension");
                    }
                } else {
                    break;
                }
            }
            this.chlen = ((int) extra[ext + 6] & 0xff) + (256 * ((int) extra[ext + 7] & 0xff));
            int chcnt = ((int) extra[ext + 8] & 0xff) + (256 * ((int) extra[ext + 9] & 0xff));
            int p = 10;
            List<Integer> lens = new ArrayList<Integer>();
            for (int i = 0; i < chcnt; i++) {
                int thischlen = ((int) extra[ext + p] & 0xff) + (256 * ((int) extra[ext + p + 1] & 0xff));
                p += 2;
                lens.add(thischlen);
            }
            int chpos = 0;
            for (Integer i : lens) {
                this.chunks.add(new Chunk(chpos, i));
                chpos += i;
            }
        } else {
            throw new Exception("Missing dictzip extension");
        }

        if ((flag & FNAME) != 0) {
            //Read and discard a null-terminated string containing the filename
            byte s = 0;
            while (true) {
                s = dictzip.readByte();
                this._firstpos += 1;
                if (s == 0)
                    break;
            }
        }

        if ((flag & FCOMMENT) != 0) {
            //Read and discard a null-terminated string containing a comment
            byte s = 0;
            while (true) {
                s = dictzip.readByte();
                this._firstpos += 1;
                if (s == 0)
                    break;
            }
        }

        if ((flag & FHCRC) != 0) {
            //Read & discard the 16-bit header CRC
            dictzip.readByte();
            dictzip.readByte();
            this._firstpos += 2;
        }
    }

    /**
     * @param n
     * @return
     * @throws Exception
     */
    private byte[] _readchunk(int n) throws Exception {
        if (n >= this.chunks.size()) {
            return null;
        }
        this.dictzip.seek(this._firstpos + this.chunks.get(n).offset);
        int size = this.chunks.get(n).size;
        byte[] buff = new byte[size];
        this.dictzip.read(buff);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        InflaterOutputStream gz = new InflaterOutputStream(bos, new Inflater(true));
        gz.write(buff);
        return bos.toByteArray();
    }

    public void runtest() {
        System.out.println("chunklen=" + this.chlen);
        System.out.println("_firstpos=" + this._firstpos);
    }

    public String test() {
        return ("chunklen=" + this.chlen);
    }


//    public static void main(String... args) {
//
//        System.gc();
//        long before = Runtime.getRuntime().freeMemory();
//        System.out.println("Mem before: " + before);
//        String k = "http://sprel401p.prod.ch4.s.com:8080/jenkins/job/greenapi305p.prod.ch3.s.com/2/console";
//        HashMap<String, Chunk> index = new HashMap<String, Chunk>();
//        for (int i = 0; i < 1000000; i++) {
//            index.put(k + i, new Chunk(i, i));
//        }
//        long after = Runtime.getRuntime().freeMemory();
//        System.out.println("Mem after: " + before);
//        System.out.println("Mem used: " + (before - after));
//        index.clear();
//    }

    public static void main(String... args) {

        String path = System.getProperty("user.home") + File.separator + "tmp";
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        // Create the set of options for appending to the file.
        Set<OpenOption> options = new HashSet<OpenOption>();
        options.add(StandardOpenOption.APPEND);
        options.add(StandardOpenOption.CREATE);

        // Create the custom permissions attribute.
        Set<PosixFilePermission> perms =
                PosixFilePermissions.fromString("rw-r-----");
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(perms);

        Path file = Paths.get(path + File.separator + "test-index.log");

        for (int ii = 0; ii < 0; ii++) {
            long start = System.currentTimeMillis();
            try (SeekableByteChannel sbc =
                         Files.newByteChannel(file, options, attr)) {
                FileChannel fch = (FileChannel) sbc;
                for (int i = 0; i < 300000000; i++) {
                    FileLock fl = null;
                    try {
                        fl = fch.lock(fch.position(), Long.MAX_VALUE - fch.position() - 1, false);
                        String rs = "proxy_asset1/proxy/img/mp/00/00/00/c0/10725c0a63189f34f66c67eb8660e625.img.v1";
                        String rs1 = "spin_prod_501206901?hei=624&wid=624&op_sharpen=1";
                        String s = "http://sprel401p.prod.ch4.s.com:8080/jenkins/job/greenapi305p.prod.ch3.s.com/2/console" + i + "\n";
                        byte data[] = s.getBytes();
                        int l = data.length;
                        long pos = fch.position();
                        int totalSize = 4 + l + 8;
                        ByteBuffer bb = ByteBuffer.allocate(totalSize);
                        bb.putInt(l);
                        bb.put(data);
                        bb.putLong(pos);
                        bb.rewind();
                        fch.write(bb);
                    } finally {
                        if (fl != null) {
                            fl.release();
                        }
                    }
                }
            } catch (IOException x) {
                System.out.println("Exception thrown: " + x);
            }
            System.out.println("Elapsed time: " + (System.currentTimeMillis() - start));
        }
        indexAndCompact(path);

    }


    private static void indexAndCompact(String path) {

        // Create the set of options for appending to the file.
        Set<OpenOption> options = new HashSet<OpenOption>();
        options.add(StandardOpenOption.APPEND);
        options.add(StandardOpenOption.CREATE);

        // Create the custom permissions attribute.
        Set<PosixFilePermission> perms =
                PosixFilePermissions.fromString("rw-r-----");
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(perms);

        HashMap<ByteArrayWrapper, Long> index = new HashMap<>();

        Path source = Paths.get(path + File.separator + "test-index.log");
        Path target = Paths.get(path + File.separator + "test-index-compacted.log");

        long start;

        try (
                FileInputStream f = new FileInputStream(source.toFile());
                FileChannel ch = f.getChannel()
        ) {
            long size = ch.size();
            System.out.println("File size: " + ch.size());
            long split = 1 + (size / Integer.MAX_VALUE);
            System.out.println("Split size: " + split);
            start = System.currentTimeMillis();
            int l = -1;
            int position = 0;
            byte[] key = null;
            long pos;
            for (int i = 0; i < split; i++) {
                System.out.println("Processing chunk: " + i);
                MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_ONLY, position, Math.min(Integer.MAX_VALUE, size - position));
                while (mb.hasRemaining()) {
                    if (mb.remaining() >= 4) {
                        if (l == -1) {
                            l = mb.getInt();
                        }
                        if (mb.remaining() >= l && key == null) {
                            key = new byte[l];
                            mb.get(key);
                            if (mb.remaining() >= 8) {
                                pos = mb.getLong();
                            } else {
                                break;
                            }
                            index.put(new ByteArrayWrapper(key), pos);
                            l = -1;
                            key = null;
                        } else {
                            position = mb.position();
                            break;
                        }
                    } else {
                        position = mb.position();
                        break;
                    }


                }

            }
            System.out.println("Building index time elapsed: " + (System.currentTimeMillis() - start));
            System.out.println("Index size: " + index.size());

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        try (FileInputStream f = new FileInputStream(source.toFile()); FileChannel ch = f.getChannel();
             SeekableByteChannel sbc =
                     Files.newByteChannel(target, options, attr)) {
            if (ch.size() > 0) {
                MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_ONLY, 0L, ch.size());
                FileChannel fch = (FileChannel) sbc;
                start = System.currentTimeMillis();
                while (mb.hasRemaining()) {
                    int l = mb.getInt();
                    byte[] key = new byte[l];
                    mb.get(key);
                    long pos = mb.getLong();
                    ByteArrayWrapper k = new ByteArrayWrapper(key);
                    if (index.containsKey(k) && index.get(k).equals(pos)) {
                        ByteBuffer bb = ByteBuffer.allocate(4 + key.length + 8);
                        bb.putInt(l);
                        bb.put(key);
                        bb.putLong(fch.position());
                        bb.rewind();
                        fch.write(bb);
                    }
                }
                System.out.println("Compaction time elapsed: " + (System.currentTimeMillis() - start));
                System.out.println("Compacted size: " + fch.size());
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

    }

    private static void index() {

    }


    static final class ByteArrayWrapper {
        private final byte[] data;

        public ByteArrayWrapper(byte[] data) {
            if (data == null) {
                throw new NullPointerException();
            }
            this.data = data;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ByteArrayWrapper)) {
                return false;
            }
            return Arrays.equals(data, ((ByteArrayWrapper) other).data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }


}



