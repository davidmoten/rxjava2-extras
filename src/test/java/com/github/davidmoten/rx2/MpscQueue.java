package com.github.davidmoten.rx2;

import java.util.concurrent.atomic.AtomicReference;

import com.github.davidmoten.guavamini.Preconditions;

public class MpscQueue<T> {

    private final AtomicReference<Node<T>> head = new AtomicReference<Node<T>>();
    private final AtomicReference<Boolean> tailSet = new AtomicReference<Boolean>(false);
    private Node<T> tail;

    private static class Node<T> {
        T value;
        Node<T> next;

        public Node(T value) {
            this.value = value;
        }
    }

    public void push(T value) {
        Preconditions.checkNotNull(value, "cannot push a null value");
        // CAS loop
        Node<T> node = new Node<T>(value);
        while (true) {
            Node<T> h = head.get();
            if (head.compareAndSet(h, node)) {
                h.next = node;
                if (tailSet.compareAndSet(false, true)) {
                    tail = h;
                }
                break;
            }
        }
    }

    public T poll() {
        if (tail == null) {
            return null;
        } else {
            Node<T> tempTail = tail;
            Node<T> next = tempTail.next;
            if (next == null) {
                tailSet.set(false);
            }
            T v = tempTail.value;
            // help gc including prevent nepotism
            tempTail.value = null;
            tail = next;
            return v;
        }
    }

}
