package com.github.davidmoten.rx2;

import java.util.concurrent.Executors;

import com.github.davidmoten.rx2.internal.schedulers.SerialSchedulerFromExecutor;

import io.reactivex.Scheduler;

public class Schedulers2 {
    
    private static final Scheduler WORK_STEALING_COMPUTATION = new SerialSchedulerFromExecutor(Executors.newWorkStealingPool());
    
    public static Scheduler workStealingComputation() {
        return WORK_STEALING_COMPUTATION;
    }

}
