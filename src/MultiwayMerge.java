import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * This class does the merging for the file after the replacement selection is
 * done
 * 
 * @author Nimay Goradia (ngoradia) and Nico Turner (nicturn)
 * @version Nov 8, 2024
 */
public class MultiwayMerge {

    private BufferPool inputBuffers;
    private BufferPool outputBuffer;
    private Buffer[] runBuffers;
    private int[] recordPositions;
    private int[] bufferPositions;
    private int[] heapIndexMap;
    private Record[] records;
    private long blocksPerRun;
    private MinHeap<Record> minHeap;

    @SuppressWarnings("resource")
    public MultiwayMerge(int runCount, String runsFile, String outputFile)
        throws FileNotFoundException,
        IOException {
        inputBuffers = new BufferPool(ByteFile.RECORDS_PER_BLOCK * 8, runsFile,
            "rw");
        outputBuffer = new BufferPool(ByteFile.RECORDS_PER_BLOCK, outputFile,
            "rw");
        runBuffers = new Buffer[runCount];
        recordPositions = new int[runCount];
        bufferPositions = new int[runCount];
        heapIndexMap = new int[runCount];
        records = new Record[runCount];
        blocksPerRun = new RandomAccessFile(new File(runsFile), runsFile)
            .length() / (runCount * ByteFile.BYTES_PER_BLOCK);

        int size = 0;
        for (int i = 0; i < runCount - 1; i++) {
            bufferPositions[i] = i * (int)blocksPerRun;
            runBuffers[i] = inputBuffers.getBuffer(bufferPositions[i]);

            if (runBuffers[i] != null) {
                records[size] = runBuffers[i].getNextRecord();
                heapIndexMap[size] = i;
                recordPositions[i] = 1;
                size++;
            }
        }
        minHeap = new MinHeap<Record>(records, size, runCount);

        while (minHeap.heapSize() > 0) {
            Record minRecord = minHeap.removeMin();
            int currentRunIndex = heapIndexMap[minHeap.heapSize()];
            outputBuffer.getBuffer().putRecord(minRecord);

            if (outputBuffer.getBuffer().isFull()) {
                outputBuffer.getBuffer().flush();
                outputBuffer = new BufferPool(ByteFile.RECORDS_PER_BLOCK,
                    runsFile, "rw");
            } 

            if (recordPositions[currentRunIndex] >= ByteFile.RECORDS_PER_BLOCK) {
                runBuffers[currentRunIndex].close();
                bufferPositions[currentRunIndex]++;

                if (bufferPositions[currentRunIndex] < blocksPerRun) {
                    //get next buffer
                    runBuffers[currentRunIndex] = inputBuffers.getBuffer(bufferPositions[currentRunIndex]);
                    recordPositions[currentRunIndex] = 0;
                }
                else
                {
                    //mark buffer as done
                    runBuffers[currentRunIndex] = null;
                }
            }
            
            if (runBuffers[currentRunIndex] != null && runBuffers[currentRunIndex].hasRemaining())
            {
                Record nextRecord = runBuffers[currentRunIndex].getNextRecord();
                minHeap.insert(nextRecord);
                heapIndexMap[minHeap.heapSize() - 1] = currentRunIndex;
                recordPositions[currentRunIndex]++;
            }
        }
        inputBuffers.close();
        outputBuffer.close();
        for (int i = 0; i < runBuffers.length; i++)
        {
            runBuffers[i].close();
        }
    }
}
