import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * BinaryParser reads through a binary file to print the first record of each
 * block.
 */
public class BinaryParser {

    private String filename;
    private Record[] allRecords;

    /**
     * Initializes the BinaryParser with the file to read.
     *
     * @param filename
     *            The name of the binary file to parse.
     */
    public BinaryParser(String filename) {
        this.filename = filename;
        allRecords = new Record[ByteFile.RECORDS_PER_BLOCK * 8];
    }


    /**
     * Reads and prints the first record of each block in the binary file.
     *
     * @throws IOException
     */
    public void printRecords() throws IOException {
        File file = new File(filename);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            byte[] recordBuffer = new byte[Record.BYTES];
            ByteBuffer bb = ByteBuffer.wrap(recordBuffer);

            int blockIndex = 1;
            int printCounter = 0;

            for (int i = 0; i < 8; i++) {
                int recordsRead = 0;

                while (recordsRead < ByteFile.RECORDS_PER_BLOCK && raf
                    .getFilePointer() < raf.length()) {
                    raf.read(recordBuffer);
                    bb.rewind();

                    long recID = bb.getLong();
                    double key = bb.getDouble();
                    Record record = new Record(recID, key);
                    allRecords[recordsRead + i * ByteFile.RECORDS_PER_BLOCK] =
                        record;
                    recordsRead++;

                    // prints the first record of each block
                    if (recordsRead == 1) {
                        System.out.print(record.getID() + " " + record.getKey() + " ");
                        blockIndex++;
                        printCounter++;
                    }
                }
                if (printCounter % 5 == 0) {
                    System.out.println();
                }

                // After processing all records in the block, move the file
                // pointer to the start of the next block
                raf.seek(i * ByteFile.BYTES_PER_BLOCK
                    + ByteFile.BYTES_PER_BLOCK);
            }
        }
    }
}
