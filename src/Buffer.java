import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Represents a single buffer for reading/writing blocks of records.
 */
class Buffer {
    private ByteBuffer buffer;
    private RandomAccessFile file;
    private boolean dirty;
    private long position;

    /**
     * Create a new buffer.
     * 
     * @param file
     *            File to read from/write to
     * @param position
     *            Starting position in file
     */
    public Buffer(RandomAccessFile file, long position) throws IOException {
        this.file = file;
        this.position = position;
        this.buffer = ByteBuffer.allocate(ByteFile.BYTES_PER_BLOCK);
        this.dirty = false;
        readBlock();
    }


    /**
     * Read a block from file into buffer.
     */
    private void readBlock() throws IOException {
        file.seek(position);
        file.read(buffer.array());
        buffer.position(0);
    }


    /**
     * Write buffer contents to file.
     */
    public void flush() throws IOException {
        if (dirty) {
            file.seek(position);
            file.write(buffer.array());
            dirty = false;
        }
    }


    /**
     * Get next record from buffer.
     */
    public Record getNextRecord() {
        long id = buffer.getLong();
        double key = buffer.getDouble();
        return new Record(id, key);
    }


    /**
     * Put a record into the buffer.
     */
    public void putRecord(Record record) {
        buffer.putLong(record.getID());
        buffer.putDouble(record.getKey());
        dirty = true;
    }


    /**
     * Check if buffer has remaining data.
     */
    public boolean hasRemaining() {
        return buffer.remaining() >= Record.BYTES;
    }


    /**
     * Check if buffer is full.
     */
    public boolean isFull() {
        return buffer.position() == buffer.capacity();
    }


    /**
     * Close the buffer, flushing if necessary.
     */
    public void close() throws IOException {
        flush();
    }
}




/**
 * Manages a pool of buffers for file I/O.
 */
class BufferPool {
    private RandomAccessFile file;
    private int maxBuffers;
    private Buffer currentBuffer;

    /**
     * Create a new buffer pool.
     * 
     * @param maxBuffers
     *            Maximum number of buffers
     * @param filename
     *            File to read from/write to
     * @param mode
     *            File access mode
     */
    public BufferPool(int maxBuffers, String filename, String mode)
        throws IOException {
        this.maxBuffers = maxBuffers;
        this.file = new RandomAccessFile(filename, mode);
    }


    /**
     * Get a buffer at the current position.
     */
    public Buffer getBuffer() throws IOException {
        return getBuffer(file.getFilePointer());
    }


    /**
     * Get a buffer at a specific position.
     */
    public Buffer getBuffer(long position) throws IOException {
        if (currentBuffer != null) {
            currentBuffer.close();
        }
        currentBuffer = new Buffer(file, position);
        return currentBuffer;
    }


    /**
     * Close the buffer pool and associated resources.
     */
    public void close() throws IOException {
        if (currentBuffer != null) {
            currentBuffer.close();
        }
        file.close();
    }
}
