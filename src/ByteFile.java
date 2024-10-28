import java.io.File;
import java.io.IOException;
import java.util.Random;
import student.TestableRandom;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Basic handling of binary data files.
 * Uses a single byte array as a buffer for disc operations
 * Each record is one long, and one double. Sorting key is the double.
 * A record is 16 bytes long, and there are 512 records per block.
 * 
 * Can be extended in several ways (writeSortedRecords()? readBlock(int)?)
 * 
 * @author CS Staff
 * @version Fall 2024
 */
public class ByteFile {
    /**
     * the number of records in one block
     */
    public final static int RECORDS_PER_BLOCK = 512;
    /**
     * the number of bytes in one record
     */
    public final static int BYTES_PER_RECORD = 16;
    /**
     * the number of bytes in one block
     */
    public final static int BYTES_PER_BLOCK = BYTES_PER_RECORD
        * RECORDS_PER_BLOCK;

    private String filename;
    private int numBlocks;

    // ----------------------------------------------------------
    /**
     * Create a new ByteFile object.
     *
     * @param filename
     *            file name
     * @param numBlocks
     *            the number of blocks in this file
     */
    public ByteFile(String filename, int numBlocks) {
        this.filename = filename;
        this.numBlocks = numBlocks;
    }


    // ----------------------------------------------------------
    /**
     * call writeRandonRecords function, and the parameter is null
     *
     * @throws IOException
     */
    public void writeRandomRecords() throws IOException {
        writeRandomRecords(null);
    }


    // ----------------------------------------------------------
    /**
     * creates a file of randomly generated records
     *
     * @param rng
     *            random variable generator
     * @throws IOException
     */
    public void writeRandomRecords(Random rng) throws IOException {
        if (rng == null) {
            rng = new TestableRandom();
        }

        byte[] basicBuffer = new byte[BYTES_PER_BLOCK];
        ByteBuffer bb = ByteBuffer.wrap(basicBuffer);
        File theFile = new File(filename);
        theFile.delete(); // Deletes all old data in file,
        // ensuring file will have only the new data

        RandomAccessFile raf = new RandomAccessFile(theFile, "rw");
        for (int block = 0; block < numBlocks; block++) {
            bb.position(0); // resets to byte position zero in ByteBuffer

            for (int rec = 0; rec < RECORDS_PER_BLOCK; rec++) {
                // puts the data in the basicBuffer...
                bb.putLong(rng.nextLong()); // a random recID
                bb.putDouble(rng.nextDouble()); // a random recKey
            }
            raf.write(basicBuffer);
            // ^^^ the slow operation! However, using one large
            // amount of data is better than using many small amounts
            bb.clear();
        }
        raf.close(); // be sure to close file
    }

    // ----------------------------------------------------------
    /**
     * checks if a file of records is sorted or not
     *
     * @return true if it is sorted, otherwise false
     * @throws IOException
     */
    public boolean isSorted() throws IOException {
        byte[] basicBuffer = new byte[BYTES_PER_BLOCK];
        ByteBuffer bb = ByteBuffer.wrap(basicBuffer);

        File theFile = new File(filename);
        RandomAccessFile raf = new RandomAccessFile(theFile, "r");
        raf.seek(0);
        Double prevRecKey = Double.MIN_VALUE;
        
        for (int block = 0; block < numBlocks; block++) {
            raf.read(basicBuffer);
            // ^^^ the slow, costly operation!!! Good thing we use buffer

            bb.position(0); // goes to byte position zero in ByteBuffer
            for (int rec = 0; rec < RECORDS_PER_BLOCK; rec++) {
                long recID = bb.getLong();
                // ^^^ reading the recID is important to advance the byteBuffer
                // position, but it is not used in the sort order
                double recKey = bb.getDouble();
                if (recKey < prevRecKey) {
                    raf.close();
                    return false;
                }
                else {
                    prevRecKey = recKey;
                }
            }
        }
        raf.close(); // be sure to close file
        return true;
    }
}
