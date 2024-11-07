import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * BinaryParser reads through a binary file to print the first record of each
 * block in sorted order.
 */
public class BinaryParser {

    private RandomAccessFile inputFile;
    private ByteBuffer inputBuffer;
    private ByteBuffer outputBuffer;
    private long beforeAndAfterRSCurrentPosition;
    private int numberOfBlocks;
    private MinHeap<Record> minHeap;
    private Record[] heap;
    private RandomAccessFile middleFile;
    private Record prevOutputBufferRecord;
    private DoubleLL runsTracker;
    private int runNumber;
    private int runLength;
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
        this.beforeAndAfterRSCurrentPosition = 0;
        this.numberOfBlocks = 0;
        this.inputFile = new RandomAccessFile(filename, "r");
        this.heap = new Record[ByteFile.RECORDS_PER_BLOCK * 8];
        this.minHeap = new MinHeap<Record>(heap, 0, heap.length);
        middleFile = new RandomAccessFile("outputFile.txt", "rw");

        prevOutputBufferRecord = null;
        DoubleLL runsTracker = new DoubleLL();
        runNumber = 0;
        runLength = 0;

        processRun();

    }


    /**
     * Reads, sorts, and prints the first record of each block in the binary
     * file.
     *
     * @throws IOException
     */
    public void processRun() throws IOException {
        // fill the first 8 blocks into the heap
        while (beforeAndAfterRSCurrentPosition < ByteFile.BYTES_PER_BLOCK * 8) {
            moveToInputBuffer();

            insertIntoHeap();

            // clear the input buffer and increment the number of blocks
            inputBuffer.clear();
            numberOfBlocks++;
        }

        replacementSelection();

        beforeAndAfterRSCurrentPosition = 0;
        int currentBlock = 0;
        while (currentBlock < numberOfBlocks) {

            moveToOutputBuffer();

            writeToMiddleFile();

            beforeAndAfterRSCurrentPosition += ByteFile.BYTES_PER_BLOCK;
            middleFile.seek(beforeAndAfterRSCurrentPosition);

            outputBuffer.clear();
            currentBlock++;
        }
        printOutput();
    }


    public void moveToInputBuffer() throws IOException {
        inputFile.seek(beforeAndAfterRSCurrentPosition);
        byte[] inputBufferHelper = new byte[ByteFile.BYTES_PER_BLOCK];
        inputFile.read(inputBufferHelper);
        inputBuffer = ByteBuffer.wrap(inputBufferHelper);
        inputBuffer.rewind();
        beforeAndAfterRSCurrentPosition += ByteFile.BYTES_PER_BLOCK;
    }


    public void insertIntoHeap() throws IOException {
        for (int i = 0; i < ByteFile.RECORDS_PER_BLOCK; i++) {
            long recID = inputBuffer.getLong();
            double key = inputBuffer.getDouble();
            Record currentRecord = new Record(recID, key);
            minHeap.insert(currentRecord);
        }
    }


    public void moveToOutputBuffer() {
        while (minHeap.heapSize() > 0) {
            Record removedRecord = minHeap.removeMin();
            minHeap.setHeapSize(minHeap.heapSize() - 1);
            long recordId = removedRecord.getID();
            double recordKey = removedRecord.getKey();
            outputBuffer.putLong(recordId);
            outputBuffer.putDouble(recordKey);
        }
    }


    public void writeToMiddleFile() throws IOException {
        outputBuffer.rewind();
        while (outputBuffer.hasRemaining()) {
            middleFile.writeByte(outputBuffer.get());
        }
        outputBuffer.clear();
        outputBuffer.rewind();
    }


    public void printOutput() throws IOException {
        int printCounter = 0;

        int i = 0;
        while (i < ByteFile.BYTES_PER_BLOCK * 8) {
            middleFile.seek(i);
            byte[] recordBytes = new byte[16];
            middleFile.read(recordBytes);
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


    public void replacementSelection() throws IOException {

        moveToInputBuffer();
        inputBuffer.rewind();

        prevOutputBufferRecord = null;

        while (inputFile.getFilePointer() != inputFile.length() && (inputBuffer
            .position() == inputBuffer.limit())) //
        {
            if (minHeap.heapSize() == 0) {
                writeToMiddleFile();
                endOfRun();
            }

            if (inputBuffer.position() == inputBuffer.limit()) {
                inputBuffer.clear();
                moveToInputBuffer();
                inputBuffer.rewind();
            }

            Record newRecord = new Record(inputBuffer.getLong(), inputBuffer
                .getDouble());

            if (prevOutputBufferRecord == null || heap[0].compareTo(
                prevOutputBufferRecord) >= 0) {
                long recordId = heap[0].getID();
                double recordKey = heap[0].getKey();
                if (outputBuffer.position() == outputBuffer.limit()) {
                    writeToMiddleFile();
                }
                outputBuffer.putLong(recordId);
                outputBuffer.putDouble(recordKey);

                minHeap.modify(0, newRecord);

                runLength++;
            }
            else {
                // removes min and disconnects and sorts the heap automatically
                // also
                minHeap.removeMin();
            }
        }

// int currentRecord = 0;
// while (currentRecord < ByteFile.RECORDS_PER_BLOCK) {
// if (minHeap.heapSize() == 0) {
// writeToOutputFile();
// minHeap.buildHeap();
// }
// else {
// Record record = heap[0];
// long recordId = record.getID();
// double recordKey = record.getKey();
// if (recordKey >= prevOutputBufferRecordKey) {
// if (!inputBuffer.hasRemaining()) {
//
// }
// outputBuffer.putLong(recordId);
// outputBuffer.putDouble(recordKey);
// prevOutputBufferRecordKey = recordKey;
// currentRecord++;
// }
// else {
// minHeap.removeMin();
// minHeap.buildHeap();
// }
// }
// }
// prevOutputBufferRecordKey = 0;

    }


    public void endOfRun() {
        minHeap = new MinHeap<Record>(heap, 0, heap.length);
        runNumber++;
        runsTracker.add(runNumber, runLength);
        runLength = 0;
    }


    /**
     * Multi-way merge function to combine sorted runs into a single sorted
     * output file.
     * Utilizes runTracker for run management.
     *
     * @throws IOException
     */
    public void multiwayMerge() throws IOException {
        inputBuffer.clear();
        outputBuffer.clear();

        // Insert the first block of each of the first 8 runs into the heap
        loadRunBlock();

        // Begin the merging process
        while (minHeap.heapSize() != 0) {
            Record minRecord = minHeap.removeMin();
            minHeap.setHeapSize(minHeap.heapSize() - 1);

            // Add minRecord to the output buffer
            outputBuffer.putLong(minRecord.getID());
            outputBuffer.putDouble(minRecord.getKey());

            // Flush the buffer if full
            if (outputBuffer.position() == outputBuffer.limit()) {
                flushOutputBuffer();
            }

            // Get the next record from the same run as minRecord
            if (!loadNextRecordFromRun(minRecord.getRunIndex())) {
                // If the run has no more records, continue with remaining runs
                continue;
            }
        }

        // Final flush for any remaining records in the output buffer
        if (outputBuffer.position() > 0) {
            flushOutputBuffer();
        }
        outputFile.close();
    }


    /**
     * Loads the first block from a specific run and adds each record to the
     * heap.
     *
     * @param runIndex
     *            Index of the run
     * @throws IOException
     */
    private void loadRunBlock() throws IOException {
        Iterator<DoubleLL.Node> iterator = runsTracker.iterator();

        // start - 1 because the start is the position and we need the index
        middleFile.seek(runsTracker.getHead().getStart() - 1);
        byte[] buffer = new byte[ByteFile.BYTES_PER_BLOCK];
        middleFile.read(buffer);
        ByteBuffer bb = ByteBuffer.wrap(buffer);

        while (iterator.hasNext()) {
            iterator.next();
        }

// while (bb.hasRemaining()) {
// long recordId = bb.getLong();
// double recordKey = bb.getDouble();
// Record record = new Record(recordId, recordKey, runIndex);
// minHeap.insert(record);
// }
    }


    /**
     * Loads the next record from the specified run into the min-heap.
     *
     * @param runIndex
     *            Index of the run
     * @return True if a new record was loaded; false if the run is fully
     *         exhausted
     * @throws IOException
     */
    private boolean loadNextRecordFromRun(int runIndex) throws IOException {
        long currentRunPosition = runsTracker.getRunCurrentPosition(runIndex);

        if (currentRunPosition < runsTracker.getRunEndPosition(runIndex)) {
            middleFile.seek(currentRunPosition);
            byte[] recordBytes = new byte[16];
            middleFile.read(recordBytes);
            ByteBuffer bb = ByteBuffer.wrap(recordBytes);

            long recordId = bb.getLong();
            double recordKey = bb.getDouble();
            Record record = new Record(recordId, recordKey, runIndex);
            minHeap.insert(record);

            // Update current position in the runTracker
            runsTracker.updateRunCurrentPosition(runIndex, currentRunPosition
                + 16);
            return true;
        }

        return false;
    }


    /**
     * Flushes the output buffer to the output file.
     *
     * @throws IOException
     */
    private void flushOutputBuffer() throws IOException {
        outputBuffer.flip();
        while (outputBuffer.hasRemaining()) {
            outputFile.write(outputBuffer.get());
        }
        outputBuffer.clear();
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

// public void insertIntoMinHeap() {
//
// }

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
}
