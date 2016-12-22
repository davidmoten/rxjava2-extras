package com.github.davidmoten.rx2.internal.flowable;

import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

/**
 * Enables a forward-only iteration of string values split by a delimiter across
 * a linked list of strings.
 */
public final class DelimitedStringLinkedList {

    private final String delimiter;
    private final StringBuilder b = new StringBuilder();

    private Node head;
    private Node tail;
    private int headPosition;
    private Node searchNode;
    private int searchPosition;
    private int nextLength;
    private boolean added;
    

    public DelimitedStringLinkedList(String delimiter) {
        this.delimiter = delimiter;
    }

    @VisibleForTesting
    int searchPosition() {
        return searchPosition;
    }

    private static final class Node {
        final String value;
        Node next;

        Node(String value, Node next) {
            this.value = value;
            this.next = next;
        }

        @Override
        public String toString() {
            return "Node [value=" + value + ", next=" + next + "]";
        }
    }

    public boolean addCalled() {
        return added;
    }

    public void add(String s) {
        added = true;
        if (s.length() == 0) {
            return;
        }
        if (head == null) {
            head = new Node(s, null);
            tail = head;
            headPosition = 0;
            searchPosition = 0;
            searchNode = head;
            nextLength = 0;
        } else {
            Node node = new Node(s, null);
            tail.next = node;
            tail = node;
            if (searchNode == null) {
                searchNode = node;
                searchPosition = 0;
            }
        }
    }

    public String remaining() {
        if (head == null) {
            return null;
        } else {
            StringBuilder b = new StringBuilder();
            Node n = head;
            do {
                if (n == head) {
                    b.append(n.value.substring(headPosition, n.value.length()));
                } else {
                    b.append(n.value);
                }
                n = n.next;
            } while (n != null);
            return b.toString();
        }
    }

    public String next() {
        while (searchNode != null) {
            if (searchNode.value.charAt(searchPosition) == delimiter.charAt(0)) {
                Node nd = searchNode;
                int pos = searchPosition + 1;
                int j = 1;
                while (j < delimiter.length()) {
                    if (pos == nd.value.length()) {
                        if (nd.next == null) {
                            break;
                        }
                        nd = nd.next;
                        pos = 0;
                    }
                    if (nd.value.charAt(pos) != delimiter.charAt(j)) {
                        break;
                    }
                    j++;
                    pos++;
                }
                boolean found = j == delimiter.length();
                if (found) {
                    // at this point (nd, pos) is the location of the end of
                    // next + delimiter
                    // (node, position) is at next + 1

                    // extract string
                    b.setLength(0);
                    b.ensureCapacity(nextLength);
                    Node n = head;
                    while (true) {
                        if (n == searchNode && n == head) {
                            b.append(n.value.substring(headPosition, searchPosition));
                            break;
                        } else if (n == head) {
                            b.append(n.value.substring(headPosition, n.value.length()));
                        } else if (n == searchNode) {
                            b.append(n.value.substring(0, searchPosition));
                            break;
                        } else {
                            b.append(n.value);
                        }
                        n = n.next;
                    }
                    if (nextLength != b.length()) {
                        throw new RuntimeException("unexpected");
                    }
                    // reset nodes and positions
                    nextLength = 0;
                    if (pos == nd.value.length()) {
                        if (tail == nd) {
                            tail = nd.next;
                        }
                        head = nd.next;
                        headPosition = 0;
                        searchPosition = 0;
                        searchNode = head;
                    } else {
                        head = nd;
                        headPosition = pos;
                        if (headPosition == head.value.length()) {
                            dropFirst();
                        }
                        searchNode = head;
                        searchPosition = headPosition;
                    }
                    return b.toString();
                } else {
                    // advance to next position

                }
            }
            nextLength++;
            searchPosition += 1;
            if (searchPosition == searchNode.value.length()) {
                if (searchNode.next == null) {
                    searchNode = null;
                    searchPosition = 0;
                    break;
                } else {
                    searchNode = searchNode.next;
                    searchPosition = 0;
                }
            }
        }
        return null;
    }

    private void dropFirst() {
        if (head.next == null) {
            tail = null;
            head = null;
            headPosition = 0;
        } else {
            if (tail == head) {
                tail = head.next;
            }
            head = head.next;
            headPosition = 0;
        }
    }

    public void clear() {
        head = null;
        tail = null;
        searchNode = null;
        headPosition = 0;
        searchPosition = 0;
    }

}
