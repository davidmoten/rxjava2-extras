package com.github.davidmoten.rx2;

import java.util.concurrent.atomic.AtomicReference;

import com.github.davidmoten.guavamini.Preconditions;

public final class MpscQueue<T> {

    private final AtomicReference<Node<T>> head = new AtomicReference<Node<T>>();
    private final AtomicReference<Boolean> tailSet = new AtomicReference<Boolean>(false);

    // mutable
    private Node<T> tail;

    private static final class Node<T> {

        // mutable

        T value;
        Node<T> next;

        Node(T value) {
            this.value = value;
        }
    }

    public void push(T value) {
        // performs one volatile read and two CAS operations per call to this method
        // (under contention can be higher)

        Preconditions.checkNotNull(value, "cannot push a null value");
        Node<T> node = new Node<T>(value);
        while (true) {
            Node<T> h = head.get();
            if (head.compareAndSet(h, node)) {
                h.next = node;
                if (tailSet.compareAndSet(false, true)) {
                    // don't mind that this is not instantly visible to poll() because
                    // in reactive operators the push is always followed by a drain that
                    // forces a poll which will be ordered after this assignment (so this
                    // assignment will be visible)
                    tail = h;
                }
                break;
            }
        }
    }

    public T poll() {
        // performs one volatile write only when reading the end of the queue

        if (tail == null) {
            return null;
        } else {
            Node<T> temp = tail;
            Node<T> next = temp.next;
            if (next == null) {
                tail = null;
                tailSet.set(false);
            } else {
                tail = next;
            }
            T v = temp.value;
            // help gc including prevent nepotism
            temp.value = null;
            return v;
        }
    }

}
