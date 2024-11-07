import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class provides the implementation for some of the methods in a doubly
 * linked list (DoubleLL).
 * It allows operations such as adding, removing, and accessing elements in the
 * list.
 * 
 * Each element is stored in a `Node` which maintains links to both the next and
 * previous elements in the list, along with the start and length of a run.
 * 
 * @author Nimay Goradia (ngoradia) and Nicolas Turner (nicturn)
 * @version 9.12.24
 */
public class DoubleLL implements Iterable<DoubleLL.Node> {

    /**
     * This private static class represents a node in the doubly linked list.
     * Each node contains the start and length of a run, and references to both
     * the next and previous nodes in the list.
     */
    public static class Node {
        private Node next; // The next node in the list
        private Node previous; // The previous node in the list
        private long start; // The start of the run
        private long length; // The length of the run

        /**
         * This constructor initializes the node with the provided start and
         * length.
         * 
         * @param start
         *            the start of the run
         * @param length
         *            the length of the run
         */
        public Node(long start, long length) {
            this.start = start;
            this.length = length;
        }


        /**
         * This method sets the reference to the next node in the list.
         * 
         * @param n
         *            the next node to be linked
         */
        public void setNext(Node n) {
            next = n;
        }


        /**
         * This method sets the reference to the previous node in the list.
         * 
         * @param n
         *            the previous node to be linked
         */
        public void setPrevious(Node n) {
            previous = n;
        }


        /**
         * This method returns the next node in the list.
         * 
         * @return the next node in the list
         */
        public Node next() {
            return next;
        }


        /**
         * This method returns the previous node in the list.
         * 
         * @return the previous node in the list
         */
        public Node previous() {
            return previous;
        }


        /**
         * This method returns the start of the run.
         * 
         * @return the start of the run
         */
        public long getStart() {
            return start;
        }


        /**
         * This method returns the length of the run.
         * 
         * @return the length of the run
         */
        public long getLength() {
            return length;
        }
    }

    private int size; // Tracks the number of elements in the list
    private Node head; // The first node in the list
    private Node tail; // The last node in the list

    /**
     * This constructor initializes an empty doubly linked list.
     */
    public DoubleLL() {
        head = null;
        tail = null;
        size = 0;
    }


    /**
     * This method returns the head node of the list.
     * 
     * @return the head node, or null if the list is empty
     */
    public Node getHead() {
        return head;
    }


    /**
     * This method returns the tail node of the list.
     * 
     * @return the tail node, or null if the list is empty
     */
    public Node getTail() {
        return tail;
    }


    /**
     * This method checks if the list is empty.
     * 
     * @return true if the list is empty, false otherwise
     */
    public boolean isEmpty() {
        return size == 0;
    }


    /**
     * This method returns the number of elements in the list.
     * 
     * @return the size of the list
     */
    public int size() {
        return size;
    }


    /**
     * This method clears the list by setting head and tail to null and
     * resetting the size.
     */
    public void clear() {
        head = null;
        tail = null;
        size = 0;
    }


    /**
     * This method checks if the list contains a specific element based on
     * start.
     * 
     * @param start
     *            the start of the run to check for
     * @return true if the element is found, false otherwise
     */
    public boolean contains(long start) {
        return lastIndexOf(start) != -1;
    }


    /**
     * This method returns the node at the specified index.
     * 
     * @param index
     *            the position of the node in the list
     * @return the node at the specified index
     * @throws IndexOutOfBoundsException
     *             if the index is out of bounds
     */
    private Node getNodeAtIndex(int index) {
        Node current = head;
        for (int i = 0; i < index; i++) {
            current = current.next();
        }
        return current;
    }


    /**
     * This method returns the last index of the specified start in the list.
     * 
     * @param start
     *            the start of the run to search for
     * @return the last index of the element, or -1 if not found
     */
    public int lastIndexOf(long start) {
        Node current = tail;
        for (int i = size() - 1; i >= 0; i--) {
            if (current.getStart() == start) {
                return i;
            }
            current = current.previous();
        }
        return -1;
    }


    /**
     * This method adds a new entry to the end of the list.
     * 
     * @param start
     *            the start of the run to be added
     * @param length
     *            the length of the run to be added
     */
    public void add(long start, long length) {
        add(size(), start, length);
    }


    /**
     * This method adds a new entry at the specified index.
     * 
     * @param index
     *            the position where the new element should be inserted
     * @param start
     *            the start of the run to be added
     * @param length
     *            the length of the run to be added
     */
    public void add(int index, long start, long length) {
        Node addition = new Node(start, length);

        if (index == 0) {
            if (head == null) {
                head = addition;
                tail = addition;
            }
            else {
                addition.setNext(head);
                head.setPrevious(addition);
                head = addition;
            }
        }
        else if (index == size) {
            tail.setNext(addition);
            addition.setPrevious(tail);
            tail = addition;
        }
        else {
            Node nodeAfter = getNodeAtIndex(index);
            Node nodeBefore = nodeAfter.previous();
            addition.setNext(nodeAfter);
            addition.setPrevious(nodeBefore);
            nodeBefore.setNext(addition);
            nodeAfter.setPrevious(addition);
        }

        size++;
    }


    /**
     * This method removes the node at the specified index.
     * 
     * @param index
     *            the position of the node to be removed
     * @return true if the removal was successful, false otherwise
     * @throws IndexOutOfBoundsException
     *             if the index is out of bounds
     */
    public boolean remove(int index) {
        Node nodeToBeRemoved = getNodeAtIndex(index);

        if (nodeToBeRemoved == head) {
            head = head.next();
            if (head != null) {
                head.setPrevious(null);
            }
        }
        else if (nodeToBeRemoved == tail) {
            tail = tail.previous();
            if (tail != null) {
                tail.setNext(null);
            }
        }
        else {
            Node before = nodeToBeRemoved.previous();
            Node after = nodeToBeRemoved.next();
            before.setNext(after);
            after.setPrevious(before);
        }

        if (size == 1) {
            head = null;
            tail = null;
        }

        size--;
        return true;
    }


    /**
     * This method returns a string representation of the list in the format
     * "{(start1, length1), (start2, length2), ...}".
     * 
     * @return a string representation of the list
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("{");
        Node currNode = head;
        while (currNode != null) {
            builder.append("(").append(currNode.getStart()).append(", ").append(
                currNode.getLength()).append(")");
            if (currNode.next() != null) {
                builder.append(", ");
            }
            currNode = currNode.next();
        }
        builder.append("}");
        return builder.toString();
    }


    @Override
    public Iterator<DoubleLL.Node> iterator() {
        return new DoubleLLIterator();
    }

    /**
     * This inner class implements an iterator for the DoubleLL class.
     */
    private class DoubleLLIterator implements Iterator<Node> {
        private Node current = head;

        /**
         * Checks if there is a next node in the list.
         *
         * @return true if there is a next node, false otherwise
         */
        @Override
        public boolean hasNext() {
            return current != null;
        }


        /**
         * Returns the next node in the list and advances the iterator.
         *
         * @return the next node
         * @throws NoSuchElementException
         *             if there is no next node
         */
        @Override
        public Node next() {
            if (!hasNext()) {
                throw new NoSuchElementException(
                    "No more elements in the list.");
            }
            Node temp = current;
            current = current.next();
            return temp;
        }


        /**
         * Returns the current node without advancing the iterator.
         *
         * @return the current node
         */
        public Node getCurrent() {
            return current;
        }
    }
}
