package com.github.davidmoten.rx2;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A multi-producer single consumer queue that is thread-safe and performant
 * under a standard drain scenario encountered in an RxJava operator.
 *
 * @param <T>
 */
public final class SpecializedMpscLinkedQueue<T> {

    private final AtomicReference<Node<T>> head = new AtomicReference<Node<T>>();
    private final AtomicReference<Boolean> tailSet = new AtomicReference<Boolean>(false);

    private SpecializedMpscLinkedQueue() {
        // constructor
    }

    public static <T> SpecializedMpscLinkedQueue<T> create() {
        return new SpecializedMpscLinkedQueue<T>();
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
        // performs one volatile read, one CAS operation and one weak CAS operation per
        // call to this method (under contention can be higher)

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
            tail = next;
            if (next == null) {
                tailSet.lazySet(false);
            }
            T v = temp.value;
            // help gc including prevent nepotism
            temp.value = null;
            temp.next = null;
            return v;
        }
    }

}
