package com.github.davidmoten.rx2.internal;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;

public final class SchedulerWithId extends Scheduler {

    private final Scheduler scheduler;
    private final String id;
    private final static Pattern pattern = Pattern.compile("\\bschedId=\\[[^\\]]+\\]+\\b");

    public SchedulerWithId(Scheduler scheduler, String id) {
        this.scheduler = scheduler;
        this.id = "[" + id + "]";
    }

    @Override
    public Worker createWorker() {

        final Worker worker = scheduler.createWorker();
        Worker w = new Worker() {

            @Override
            public Disposable schedule(final Runnable action, long delayTime, TimeUnit unit) {
                Runnable a = new Runnable() {
                    @Override
                    public void run() {
                        String name = null;
                        try {
                            name = setThreadName();
                            action.run();
                        } finally {
                            if (name != null) {
                                Thread.currentThread().setName(name);
                            }
                        }
                    }
                };
                return worker.schedule(a, delayTime, unit);
            }

            @Override
            public void dispose() {
                worker.dispose();
            }

            @Override
            public boolean isDisposed() {
                return worker.isDisposed();
            }

        };
        return w;

    }

    private String setThreadName() {
        String name = Thread.currentThread().getName();
        String newName = updateNameWithId(name, id);
        Thread.currentThread().setName(newName);
        return name;
    }

    private static String updateNameWithId(String name, String id) {
        final String newName;
        if (name == null) {
            newName = id;
        } else {
            Matcher matcher = pattern.matcher(name);
            if (matcher.find()) {
                newName = name.replace(matcher.group(), "schedId=" + id);
            } else {
                newName = name + "|schedId=" + id;
            }
        }
        return newName;
    }

}
