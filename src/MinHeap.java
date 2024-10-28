// Min-heap implementation by CS Staff, based on OpenDSA Heap code
// Can use `java -ea` (Java's VM arguments) to Enable Assertions
// These assertions will check valid heap positions

// -------------------------------------------------------------------------
/**
 * Here is a class to implement MinHeap functions
 * 
 * @param <T>
 *            data type in this minHeap
 * @author CS Staff
 * @version Fall 2024
 */
class MinHeap<T extends Comparable<T>> {
    private T[] heap; // Pointer to the heap array
    private int capacity; // Maximum size of the heap
    private int n; // Number of active things currently in heap

    // ----------------------------------------------------------
    /**
     * Create a new MinHeap object.
     *
     * @param arrayForHeap
     *            array
     * @param heapSize
     *            heap size
     * @param capacity
     *            capacity
     */
    // Constructor supporting preloading of heap contents
    MinHeap(T[] arrayForHeap, int heapSize, int capacity) {
        assert capacity <= arrayForHeap.length : "capacity is"
            + " beyond array limits";
        assert heapSize <= capacity : "Heap size is beyond max";
        heap = arrayForHeap;
        n = heapSize;
        this.capacity = capacity;
        buildHeap();
    }


    /**
     * Return position for left child of pos
     *
     * @param pos
     *            position
     * @return position of leftChild
     */
    public static int leftChild(int pos) {
        return 2 * pos + 1;
    }
    

    /**
     * Return position for right child of pos
     *
     * @param pos
     *            position
     * @return position of rightChild
     */
    public static int rightChild(int pos) {
        return 2 * pos + 2;
    }


    /**
     * own descriptive comment
     *
     * @param pos
     *            position
     * @return position of parent
     */

    public static int parent(int pos) {
        return (pos - 1) / 2;
    }


    /**
     * Forcefully changes the heap size. May need a buildHeap() afterwards
     *
     * @param newSize
     *            new size
     */
    public void setHeapSize(int newSize) {
        n = newSize;
    }
    
    /**
     * Return current size of the heap
     *
     * @return current heapSize 
     */
    // Return current size of the heap
    public int heapSize() {
        return n;
    }

    /**
     * own descriptive comment
     *
     * @param pos
     *            position
     * @return boolean
     */

    public boolean isLeaf(int pos) {
        return (n / 2 <= pos) && (pos < n);
    }


    // ----------------------------------------------------------
    /**
     * Insert val into heap
     *
     * @param key
     *            key
     */
    public void insert(T key) {
        assert n < capacity : "Heap is full; cannot insert";
        heap[n] = key;
        n++;
        siftUp(n - 1);
    }


    // ----------------------------------------------------------
    /**
     * Organize contents of array to satisfy the heap structure
     */
    public void buildHeap() {
        // Call sift down on each internal node, starting from bottom
        for (int i = parent(n - 1); i >= 0; i--) {
            siftDown(i);
        }
    }


    // ----------------------------------------------------------
    /**
     * Moves an element down to its correct place
     *
     * @param pos
     *            position
     */
    public void siftDown(int pos) {
        assert (0 <= pos && pos < n) : "Invalid heap position";
        while (!isLeaf(pos)) {
            int child = leftChild(pos);
            // compare the left and right children
            if ((child + 1 < n) && isLessThan(child + 1, child)) {
                child = child + 1; // child is now the index with the smaller
                                   // value
            }
            if (!isLessThan(child, pos)) {
                return; // stop early
            }
            swap(pos, child);
            pos = child; // keep sifting down
        }
    }


    // ----------------------------------------------------------
    /**
     * Moves an element up to its correct place
     *
     * @param pos
     *            position
     */
    public void siftUp(int pos) {
        assert (0 <= pos && pos < n) : "Invalid heap position";
        while (pos > 0) {
            int parent = parent(pos);
            if (isLessThan(parent, pos)) {
                return; // stop early
            }
            swap(pos, parent);
            pos = parent; // keep sifting up
        }
    }


    /**
     * Remove and return minimum value
     *
     * @return minimum value
     */
    public T removeMin() {
        assert n > 0 : "Heap is empty; cannot remove";
        n--;
        if (n > 0) {
            swap(0, n); // Swap minimum with last value
            siftDown(0); // Put new heap root val in correct place
        }
        return heap[n];
    }


    // ----------------------------------------------------------
    /**
     * Remove and return element at specified position
     *
     * @param pos
     *            position
     * @return removed data
     */
    public T remove(int pos) {
        assert (0 <= pos && pos < n) : "Invalid heap position";
        n--;
        if (n > 0) {
            swap(pos, n); // Swap with last value
            update(pos); // Move other value to correct position
        }
        return heap[n];
    }
    

    /**
     * Modify the value at the given position, then sift it around
     * 
     * @param pos
     *            the position in the MinHeap
     * @param newVal
     *            the new value
     */
    public void modify(int pos, T newVal) {
        assert (0 <= pos && pos < n) : "Invalid heap position";
        heap[pos] = newVal;
        update(pos);
    }


    /**
     * The value at pos has been changed, restore the heap property
     * 
     * @param pos
     *            the position in the MinHeap
     */
    private void update(int pos) {
        siftUp(pos); // priority goes up
        siftDown(pos); // unimportant goes down
    }


    /**
     * swaps the elements at two positions
     * 
     * @param pos1
     *            one of the position need to swap in the MinHeap
     * 
     * @param pos2
     *            another position need to swap in the MinHeap
     */

    private void swap(int pos1, int pos2) {
        T temp = heap[pos1];
        heap[pos1] = heap[pos2];
        heap[pos2] = temp;
    }


    /**
     * does fundamental comparison used for checking heap validity
     * 
     * @param pos1
     *            one of the position need to compare in the MinHeap
     * @param pos2
     *            another position need to compare in the MinHeap
     * @return boolean
     */
    private boolean isLessThan(int pos1, int pos2) {
        return heap[pos1].compareTo(heap[pos2]) < 0;
    }
}
