package com.github.davidmoten.rx2;

import java.util.concurrent.atomic.AtomicReference;

public final class MpscQueue<T> {

    private final AtomicReference<Node<T>> head = new AtomicReference<Node<T>>();
    private final AtomicReference<Boolean> tailSet = new AtomicReference<Boolean>(false);

    public MpscQueue() {
        // constructor
    }

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

    public void offer(T value) {
        // performs one volatile read and two CAS operations per call to this method
        // (under contention can be higher)

        Node<T> node = new Node<T>(value);
        while (true) {
            Node<T> h = head.get();
            if (head.compareAndSet(h, node)) {
                if (h != null) {
                    h.next = node;
                    if (tailSet.weakCompareAndSet(false, true)) {
                        // don't mind that this is not instantly visible to poll() because
                        // in reactive operators the push is always followed by a drain that
                        // forces a poll which will be ordered after this assignment (so this
                        // assignment will be visible)
                        tail = h;
                    }
                } else if (tailSet.weakCompareAndSet(false, true)) {
                    tail = node;
                }
                break;
            }
        }
    }

    public T poll() {
        // performs one lazy set only when reading the end of the queue

        if (tail == null) {
            return null;
        } else {
            Node<T> temp = tail;
            Node<T> next = temp.next;
            if (next == null) {
                tail = null;
                tailSet.lazySet(false);
            } else {
                tail = next;
            }
            T v = temp.value;
            // help gc including prevent nepotism
            temp.value = null;
            temp.next = null;
            return v;
        }
    }

}
