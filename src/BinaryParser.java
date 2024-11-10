import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class BinaryParser {
    private static final int HEAP_BLOCKS = 8;
    private RandomAccessFile inputFile;
    private RandomAccessFile runFile;
    private Buffer inputBuffer;
    private Buffer outputBuffer;
    private MinHeap<Record> heap;
    private Record[] heapArray;
    private DoubleLL runs; // Track run positions

    public BinaryParser(String filename) throws IOException {
        this.inputFile = new RandomAccessFile(filename, "rw");
        this.runFile = new RandomAccessFile("runFile.bin", "rw");
        this.inputBuffer = new Buffer(inputFile, 0);
        this.outputBuffer = new Buffer(runFile, 0);
        this.heapArray = new Record[ByteFile.RECORDS_PER_BLOCK * HEAP_BLOCKS];
        this.heap = new MinHeap<>(heapArray, 0, heapArray.length);
        this.runs = new DoubleLL();
    }


    public void printRecords() throws IOException {
        // Phase 1: Create sorted runs using replacement selection
        createSortedRuns();

        // Phase 2: Merge runs until we have a single sorted file
        multiwayMerge();

        // Print the first record of each block
        printOutput();

        // Cleanup
        cleanup();
    }


    private void createSortedRuns() throws IOException {
        long runStart = 0;
        long currentRunLength = 0;

        // Initialize heap with first HEAP_BLOCKS worth of records
        fillInitialHeap();

        while (true) {
            if (heap.heapSize() == 0) {
                // End of current run
                if (currentRunLength > 0) {
                    runs.add(runStart, currentRunLength);
                }
                if (!hasMoreRecords()) {
                    break;
                }
                runStart = runFile.getFilePointer();
                currentRunLength = 0;
                fillInitialHeap();
            }

            // Get smallest record from heap
            Record minRecord = heap.removeMin();
            outputBuffer.putRecord(minRecord);
            currentRunLength++;

            if (outputBuffer.isFull()) {
                outputBuffer.flush();
                outputBuffer = new Buffer(runFile, runFile.getFilePointer());
            }

            // Try to read next record
            if (hasMoreRecords()) {
                Record nextRecord = inputBuffer.getNextRecord();
                if (!inputBuffer.hasRemaining()) {
                    inputBuffer = new Buffer(inputFile, inputFile
                        .getFilePointer());
                }

                // If next record can be part of current run, add to heap
                if (nextRecord.getKey() >= minRecord.getKey()) {
                    heap.insert(nextRecord);
                }
                else {
                    // Save for next run
                    heap.setHeapSize(heap.heapSize() + 1);
                    heapArray[heap.heapSize() - 1] = nextRecord;
                }
            }
        }
    }


    private void multiwayMerge() throws IOException {
        while (runs.size() > 1) {
            DoubleLL newRuns = new DoubleLL();
            DoubleLL.Node currentRun = runs.getHead();

            while (currentRun != null) {
                // Merge up to HEAP_BLOCKS runs at a time
                Buffer[] runBuffers = new Buffer[HEAP_BLOCKS];
                int runCount = 0;
                long mergeStart = runFile.getFilePointer();
                long mergeLength = 0;

                // Initialize buffers for current merge
                for (int i = 0; i < HEAP_BLOCKS && currentRun != null; i++) {
                    runBuffers[i] = new Buffer(runFile, currentRun.getStart());
                    heap.insert(runBuffers[i].getNextRecord());
                    runCount++;
                    currentRun = currentRun.next();
                }

                // Merge runs
                while (heap.heapSize() > 0) {
                    Record minRecord = heap.removeMin();
                    outputBuffer.putRecord(minRecord);
                    mergeLength++;

                    if (outputBuffer.isFull()) {
                        outputBuffer.flush();
                        outputBuffer = new Buffer(runFile, runFile
                            .getFilePointer());
                    }
 
                    // Try to get next record from same run
                    for (int i = 0; i < runCount; i++) {
                        if (runBuffers[i].hasRemaining()) {
                            heap.insert(runBuffers[i].getNextRecord());
                            if (!runBuffers[i].hasRemaining()
                                && runBuffers[i].getPosition() < currentRun
                                    .getStart() + currentRun.getLength()) {
                                runBuffers[i] = new Buffer(runFile,
                                    runBuffers[i].getPosition()
                                        + ByteFile.BYTES_PER_BLOCK);
                            }
                        }
                    }
                }

                // Add new merged run
                if (mergeLength > 0) {
                    newRuns.add(mergeStart, mergeLength);
                }
            }

            runs = newRuns;
        }

        // Copy final run back to input file
        if (runs.size() == 1) {
            copyRunToInput(runs.getHead());
        }
    }


    private void copyRunToInput(DoubleLL.Node run) throws IOException {
        inputFile.seek(0);
        runFile.seek(run.getStart());

        byte[] buffer = new byte[ByteFile.BYTES_PER_BLOCK];
        long remaining = run.getLength() * Record.BYTES;

        while (remaining > 0) {
            int toRead = (int)Math.min(buffer.length, remaining);
            runFile.read(buffer, 0, toRead);
            inputFile.write(buffer, 0, toRead);
            remaining -= toRead;
        }
    }


    private void fillInitialHeap() throws IOException {
        heap.setHeapSize(0);
        while (heap.heapSize() < heapArray.length && hasMoreRecords()) {
            Record record = inputBuffer.getNextRecord();
            heap.insert(record);
            if (!inputBuffer.hasRemaining()) {
                inputBuffer = new Buffer(inputFile, inputFile.getFilePointer());
            }
        }
    }


    private boolean hasMoreRecords() throws IOException {
        return inputFile.getFilePointer() < inputFile.length() || inputBuffer
            .hasRemaining();
    }


    private void printOutput() throws IOException {
        inputFile.seek(0);
        Buffer printBuffer = new Buffer(inputFile, 0);
        int recordCounter = 0;

        while (inputFile.getFilePointer() < inputFile.length()) {
            if (printBuffer.hasRemaining()) {
                Record record = printBuffer.getNextRecord();
                System.out.print(record.getID() + " " + record.getKey());
                recordCounter++;

                if (recordCounter % 5 == 0) {
                    System.out.println();
                }
                else if (inputFile.getFilePointer() < inputFile.length()) {
                    System.out.print(" ");
                }

                // Skip to next block's first record
                printBuffer = new Buffer(inputFile, inputFile.getFilePointer()
                    + ByteFile.BYTES_PER_BLOCK - Record.BYTES);
            }
            else {
                printBuffer = new Buffer(inputFile, inputFile.getFilePointer());
            }
        }
        if (recordCounter % 5 != 0) {
            System.out.println();
        }
    }


    private void cleanup() throws IOException {
        inputFile.close();
        runFile.close();
        new File("runFile.bin").delete();
    }
}
