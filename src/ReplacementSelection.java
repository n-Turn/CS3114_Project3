import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * This class does the replacement selection for the file
 * 
 * @author Nimay Goradia (ngoradia) and Nico Turner (nicturn)
 * @version Nov 8, 2024
 */
public class ReplacementSelection {

    private BufferPool inputBuffer;
    private BufferPool outputBuffer;
    private int heapSize;
    private Record[] heap;
    private MinHeap<Record> minHeap;
    private int heapLastIndex;
    private double currentRecordKey;

    public ReplacementSelection(String inputFile, String runsFile, int runCount)
        throws IOException {
        this.inputBuffer = new BufferPool(ByteFile.RECORDS_PER_BLOCK, inputFile,
            "rw");
        this.outputBuffer = new BufferPool(ByteFile.RECORDS_PER_BLOCK, runsFile,
            "rw");
        this.heapSize = ByteFile.RECORDS_PER_BLOCK * 8;
        this.heap = new Record[heapSize];
        this.heapLastIndex = ByteFile.RECORDS_PER_BLOCK * 8 - 1;
        this.minHeap = new MinHeap<Record>(this.heap, 0, heap.length);

        this.currentRecordKey = Double.NEGATIVE_INFINITY;

        int i = 0;
        while (i < heap.length && inputBuffer.getBuffer().hasRemaining()) {
            Record newRecord = inputBuffer.getBuffer().getNextRecord();
            heap[i] = newRecord;
            i++;
        }

        this.minHeap = new MinHeap<Record>(this.heap, i, heap.length);
        System.out.println(minHeap.heapSize());

        while (minHeap.heapSize() > 0) {
            Record minRecord = minHeap.removeMin();
            outputBuffer.getBuffer().putRecord(minRecord);
            currentRecordKey = minRecord.getKey();

            if (outputBuffer.getBuffer().isFull()) {
                outputBuffer.getBuffer().flush();
                outputBuffer = new BufferPool(ByteFile.RECORDS_PER_BLOCK,
                    runsFile, "rw");
            }

            if (inputBuffer.getBuffer().hasRemaining()) {
                Record newRecord = inputBuffer.getBuffer().getNextRecord();
                if (newRecord.getKey() < currentRecordKey) {
                    heap[heapLastIndex] = newRecord;
                    heapLastIndex--;
                }
                else {
                    minHeap.insert(newRecord);
                }
            }

            if (minHeap.heapSize() == 0 && heapLastIndex < heapSize - 1) {
                System.arraycopy(heap, heapLastIndex + 1, heap, 0, heap.length
                    - heapLastIndex - 1);
                this.minHeap = new MinHeap<Record>(this.heap, 0, heap.length);
                this.currentRecordKey = Double.NEGATIVE_INFINITY;
                this.heapLastIndex = ByteFile.RECORDS_PER_BLOCK * 8 - 1;
                runCount++;
            }
        }
        inputBuffer.close();
        outputBuffer.close();
    }
}
