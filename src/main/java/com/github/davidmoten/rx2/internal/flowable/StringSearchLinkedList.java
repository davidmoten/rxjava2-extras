package com.github.davidmoten.rx2.internal.flowable;

public class StringSearchLinkedList {

    private final String delimiter;

    private Node head;
    private Node tail;
    private int headPosition;
    private Node searchNode;
    private int searchPosition;
    private int nextLength;

    public StringSearchLinkedList(String delimiter) {
        this.delimiter = delimiter;
    }

    int headPosition() {
        return headPosition;
    }

    int searchPosition() {
        return searchPosition;
    }

    int nextLength() {
        return nextLength;
    }

    private static class Node {
        final String value;
        Node next;

        Node(String value, Node next) {
            this.value = value;
            this.next = next;
        }
    }

    public void add(String s) {
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
            if (searchNode == null) {
                searchNode = node;
                searchPosition = 0;
            }
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
                    StringBuilder b = new StringBuilder(nextLength);
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
                    // reset nodes and positions
                    nextLength = 0;
                    if (pos == nd.value.length() - 1) {
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
        }
    }

}
