import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * BinaryParser reads through a binary file to print the first record of each
 * block in sorted order.
 */
public class BinaryParser {

    private RandomAccessFile raf;
    private ByteBuffer inputBuffer;
    private ByteBuffer outputBuffer;
    private long currentPosition;
    private int numberOfBlocks;
    private MinHeap<Record> minHeap;
    private Record[] heap;
    private RandomAccessFile outputFile;

    /**
     * Initializes the BinaryParser with the file to read.
     *
     * @param filename
     *            The name of the binary file to parse.
     * @throws IOException
     */
    public BinaryParser(String filename) throws IOException {
        this.inputBuffer = ByteBuffer.allocate(ByteFile.BYTES_PER_BLOCK);
        this.outputBuffer = ByteBuffer.allocate(ByteFile.BYTES_PER_BLOCK);
        this.currentPosition = 0;
        this.numberOfBlocks = 0;
        this.raf = new RandomAccessFile(filename, "r");
        this.heap = new Record[ByteFile.RECORDS_PER_BLOCK * 8];
        this.minHeap = new MinHeap<Record>(heap, 0, heap.length);
        outputFile = new RandomAccessFile("outputFile.txt", "rw");
    }


    /**
     * Reads, sorts, and prints the first record of each block in the binary
     * file.
     *
     * @throws IOException
     */
    public void printRecords() throws IOException {
        while (currentPosition < raf.length()
            && currentPosition < ByteFile.BYTES_PER_BLOCK * 8) {
            raf.seek(currentPosition);
            byte[] inputBufferHelper = new byte[ByteFile.BYTES_PER_BLOCK];
            raf.read(inputBufferHelper);
            inputBuffer = ByteBuffer.wrap(inputBufferHelper);
            inputBuffer.rewind();
            currentPosition += ByteFile.BYTES_PER_BLOCK;

            for (int i = 0; i < ByteFile.RECORDS_PER_BLOCK; i++) {
                long recID = inputBuffer.getLong();
                double key = inputBuffer.getDouble();
                Record currentRecord = new Record(recID, key);
                minHeap.insert(currentRecord);
            }
            inputBuffer.clear();
            numberOfBlocks++;
        }

        currentPosition = 0;
        int currentBlock = 0;
        while (currentBlock < numberOfBlocks) {
            int currentRecord = 0;
            while (currentRecord < ByteFile.RECORDS_PER_BLOCK) {
                Record removedRecord = minHeap.removeMin();
                long recordId = removedRecord.getID();
                double recordKey = removedRecord.getKey();
                outputBuffer.putLong(recordId);
                outputBuffer.putDouble(recordKey);
                currentRecord++;
            }
            outputBuffer.rewind();
            while (outputBuffer.hasRemaining()) {
                outputFile.writeByte(outputBuffer.get());
            }
            currentPosition += ByteFile.BYTES_PER_BLOCK;
            outputFile.seek(currentPosition);

            outputBuffer.clear();
            currentBlock++;
        }
        printOutput();
    }

// /**
// * Replacement selection for the minheap
// */
// public void replacementSelection()
// {
// byte[] inputBuffer = new byte[ByteFile.BYTES_PER_RECORD]; //8192 = 512 x 16
//
// inputBuffer = insertIntoInputBuffer(inputBuffer);
//
// while()
// insertIntoMinHeap();
// }

// public void insertIntoInputBuffer() throws IOException {
// byte[] inputBufferHelper = new byte[ByteFile.BYTES_PER_RECORD];
// raf.seek(currentPosition);
// raf.read(inputBufferHelper);
// inputBuffer = ByteBuffer.wrap(inputBufferHelper);
// currentPosition += ByteFile.BYTES_PER_BLOCK;
//
// }


//    public void insertIntoMinHeap() {
//
//    }

// public void printOutput() throws IOException {
// long targetSize = ByteFile.BYTES_PER_BLOCK * 8;
// raf.setLength(targetSize);
//
// int printCounter = 0;
//
// while (outputFile.getFilePointer() < outputFile.length()) {
// long recordId = outputFile.readLong();
// double recordKey = outputFile.readDouble();
//
// // Append the record to the output
// System.out.println(recordId + " " + recordKey);
// printCounter++;
//
// // Print a new line every 5 records
// if (printCounter % 5 == 0) {
// System.out.println();
// }
// else {
// System.out.println(" ");
// }
// }
// }


    public void printOutput() throws IOException {        
        int printCounter = 0;
        
        int i = 0;
        while (i < ByteFile.BYTES_PER_BLOCK * 8) {
            outputFile.seek(i);
            byte[] recordBytes = new byte[16];
            outputFile.read(recordBytes);
            ByteBuffer printBuffer = ByteBuffer.wrap(recordBytes);
            // Append the record to the output
            System.out.print(printBuffer.getLong() + " " + printBuffer
                .getDouble());
            printCounter++;

            if (printCounter % 5 == 0) {
                System.out.println();
            }
            else {
                System.out.print(" ");
            }
            i += (ByteFile.RECORDS_PER_BLOCK * 16);
        }
    }
}
