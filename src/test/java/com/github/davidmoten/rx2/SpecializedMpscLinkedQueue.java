package com.github.davidmoten.rx2;

import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.internal.queue.MpscLinkedQueue;

/**
 * A multi-producer single consumer queue that is thread-safe and performant
 * under a standard drain scenario encountered in an RxJava operator.
 *
 * @param <T>
 */
public final class SpecializedMpscLinkedQueue<T> {

    private final AtomicReference<HeadTail<T>> headTail = new AtomicReference<HeadTail<T>>();

    private SpecializedMpscLinkedQueue() {
        // constructor
    }

    public static <T> SpecializedMpscLinkedQueue<T> create() {
        return new SpecializedMpscLinkedQueue<T>();
    }

    // mutable
    private Node<T> head;

    @SuppressWarnings("serial")
    private static final class Node<T> extends AtomicReference<Node<T>> {
        // this.get is next

        // mutable
        T value;

        Node(T value) {
            this.value = value;
        }

        Node<T> next() {
            return get();
        }
    }

    private static final class HeadTail<T> {
        final Node<T> head;
        final Node<T> tail;

        HeadTail(Node<T> head, Node<T> tail) {
            this.head = head;
            this.tail = tail;
        }
    }

    public void offer(T value) {
        // performs one volatile read, one CAS operation and one weak CAS operation per
        // call to this method (under contention can be higher)

        Node<T> node = new Node<T>(value);
        while (true) {
            HeadTail<T> ht = headTail.get();
            final HeadTail<T> ht2;
            if (ht.head == null) {
                if (ht.tail == null) {
                    ht2 = new HeadTail<T>(node, node);
                } else {
                    // ?
                    throw new RuntimeException("unexpected");
                }
            } 
        }

    }

    public T poll() {
        return null;
    }

    MpscLinkedQueue<T> q;

}
