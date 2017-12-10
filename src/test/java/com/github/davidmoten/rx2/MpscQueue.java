package com.github.davidmoten.rx2;

import java.util.concurrent.atomic.AtomicReference;

import com.github.davidmoten.guavamini.Preconditions;

public class MpscQueue<T> {

    private final AtomicReference<Node<T>> head = new AtomicReference<Node<T>>();
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
        while (true) {
            Node<T> h = head.get();
            Node<T> node = new Node<T>(value);
            if (head.compareAndSet(h, node)) {
                h.next = node;
                if (tail == null) {
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
            Node<T> next = tail.next;
            T v = tail.value;
            // help gc including prevent nepotism
            tail.value = null;
            tail = next;
            return v;
        }
    }

}
