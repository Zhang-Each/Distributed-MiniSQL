package miniSQL.BUFFERMANAGER;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class BufferManager {

    private static final int MAXBLOCKNUM = 50;  //maximum block numbers
    private static final int EOF = -1; //none-exist num
    public static Block[] buffer = new Block[MAXBLOCKNUM];  //buffer

    public BufferManager() {
        //do nothing
    }

    public static void initialBuffer() {
        for (int i = 0; i < MAXBLOCKNUM; i++)
            buffer[i] = new Block();  //allocate new memory for blocks
    }

    public static void testInterface() {
        Block b = new Block();
        b.writeInteger(1200, 2245);
        b.writeFloat(76, (float) 2232.14);
        b.writeString(492, "!!!httnb!");
        b.setFilename("buffer_test");
        b.setBlockOffset(15);
        buffer[1] = b;
        writeBlockToDisk(1);
    }

    public static void destructBufferManager() {
        for (int i = 0; i < MAXBLOCKNUM; i++)
            if (buffer[i].valid()) writeBlockToDisk(i); //write back to disk if it's valid
    }

    public static void makeInvalid(String filename) {
        for (int i = 0; i < MAXBLOCKNUM; i++)
            if (buffer[i].getFilename() != null && buffer[i].getFilename().equals(filename))
                buffer[i].valid(false);
    }

    //if the block exist and it's valid, return this block else return a empty block
    public static int readBlockFromDisk(String filename, int ofs) {
        int i, j;
        for (i = 0; i < MAXBLOCKNUM; i++)  //find the target block
            if (buffer[i].valid() && buffer[i].getFilename().equals(filename)
                    && buffer[i].getBlockOffset() == ofs) return i;
        File file = new File(filename); //block does not found
        int bid = getFreeBlockId();
        try {
            if (bid == EOF) return EOF; //there are no free blocks
            if (!file.exists()) file.createNewFile();  //if not exists such file
            if (!readBlockFromDisk(filename, ofs, bid)) return EOF;
        } catch (Exception e) {
            return EOF;
        }
        return bid;
    }

    //if the block exist and it's valid, return this block else return a empty block
    public static Block readBlockFromDiskQuote(String filename, int ofs) {
        int i, j;
        for (i = 0; i < MAXBLOCKNUM; i++)  //find the target block
            if (buffer[i].valid() && buffer[i].getFilename().equals(filename)
                    && buffer[i].getBlockOffset() == ofs) break;
        if (i < MAXBLOCKNUM) {  //there exist a block
            return buffer[i];
        } else { //block does not found
            File file = new File(filename);
            int bid = getFreeBlockId();
            if (bid == EOF || !file.exists()) return null; //there are no free blocks
            if (!readBlockFromDisk(filename, ofs, bid)) return null;
            return buffer[bid];
        }
    }

    private static boolean readBlockFromDisk(String filename, int ofs, int bid) {
        boolean flag = false;  //check whether operation is success
        byte[] data = new byte[Block.BLOCKSIZE];  //temporary data
        RandomAccessFile raf = null;  //to seek the position for data to write
        try {
            File in = new File(filename);
            raf = new RandomAccessFile(in, "rw");
            if ((ofs + 1) * Block.BLOCKSIZE <= raf.length()) {  //if the block is in valid position
                raf.seek(ofs * Block.BLOCKSIZE);
                raf.read(data, 0, Block.BLOCKSIZE);
            } else {  //when overflow it returns an empty block
                Arrays.fill(data, (byte) 0);
            }
            flag = true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (raf != null) raf.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        if (flag) { //must reset all modes and data after successfully handle the file
            buffer[bid].resetModes();
            buffer[bid].setBlockData(data);
            buffer[bid].setFilename(filename);
            buffer[bid].setBlockOffset(ofs);
            buffer[bid].valid(true);  //make it valid
        }
        return flag;
    }

    private static boolean writeBlockToDisk(int bid) {
        if (!buffer[bid].dirty()) {  //block is valid but does not be modified
            buffer[bid].valid(false);  //only to make it invalid
            return true;
        }
        RandomAccessFile raf = null;  //to seek the position for data to write
        try {
            File out = new File(buffer[bid].getFilename());
            raf = new RandomAccessFile(out, "rw");
            if (!out.exists()) out.createNewFile();  //if file does not exist
            raf.seek(buffer[bid].getBlockOffset() * Block.BLOCKSIZE);
            raf.write(buffer[bid].getBlockData());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        } finally {
            try {
                if (raf != null) raf.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
                return false;
            }
        }
        buffer[bid].valid(false);  //make it invalid
        return true;
    }

    private static int getFreeBlockId() {
        int i;
        int index = EOF;  //-1 for none free block exist
        int mincount = 0x7FFFFFFF;  //initialize with maximum integer
        for (i = 0; i < MAXBLOCKNUM; i++) {
            if (!buffer[i].lock() && buffer[i].getLRU() < mincount) {
                index = i;
                mincount = buffer[i].getLRU();
            }
        }
        if (index != EOF && buffer[index].dirty())  //if the block is dirty
            writeBlockToDisk(index);
        return index;
    }

}
