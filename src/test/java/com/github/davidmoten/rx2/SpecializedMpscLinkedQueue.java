package com.github.davidmoten.rx2;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A multi-producer single consumer queue that is thread-safe and performant
 * under a standard drain scenario encountered in an RxJava operator.
 *
 * @param <T>
 */
public final class SpecializedMpscLinkedQueue<T> {

    private final AtomicReference<Node<T>> tail = new AtomicReference<Node<T>>();
    private final AtomicReference<Boolean> headSet = new AtomicReference<Boolean>(false);

    private SpecializedMpscLinkedQueue() {
        // constructor
    }

    public static <T> SpecializedMpscLinkedQueue<T> create() {
        return new SpecializedMpscLinkedQueue<T>();
    }

    // mutable
    private Node<T> head;

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
            Node<T> h = tail.get();
            if (tail.compareAndSet(h, node)) {
                if (h != null) {
                    h.next = node;
                    if (headSet.weakCompareAndSet(false, true)) {
                        // don't mind that this is not instantly visible to poll() because
                        // in reactive operators the push is always followed by a drain that
                        // forces a poll which will be ordered after this assignment (so this
                        // assignment will be visible)
                        head = h;
                    }
                } else if (headSet.weakCompareAndSet(false, true)) {
                    head = node;
                }
                break;
            }
        }
    }

    public T poll() {
        // performs one lazy set only when reading the end of the queue

        if (head == null) {
            return null;
        } else {
            Node<T> temp = head;
            Node<T> next = temp.next;
            head = next;
            if (next == null) {
                headSet.lazySet(false);
            }
            T v = temp.value;
            // help gc including prevent nepotism
            temp.value = null;
            temp.next = null;
            return v;
        }
    }

}
