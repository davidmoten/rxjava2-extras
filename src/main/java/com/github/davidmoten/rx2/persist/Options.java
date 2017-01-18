package com.github.davidmoten.rx2.persist;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.rx2.buffertofile.DataSerializer;
import com.github.davidmoten.rx2.buffertofile.Serializer;
import com.github.davidmoten.rx2.buffertofile.Serializers;
import com.github.davidmoten.rx2.internal.flowable.FlowablePersisting;

import io.reactivex.Flowable;

public final class Options {

    private final File directory;
    private final String prefix;
    private final long startTime;
    private final long retentionPeriodMs;

    public Options(File directory, String prefix, long startTime, long retentionPeriodMs) {
        Preconditions.checkNotNull(directory);
        Preconditions.checkNotNull(prefix);
        Preconditions.checkArgument(retentionPeriodMs > 0);
        this.directory = directory;
        this.prefix = prefix;
        this.startTime = startTime;
        this.retentionPeriodMs = retentionPeriodMs;
    }

    public File directory() {
        return directory;
    }

    public String prefix() {
        return prefix;
    }

    public long startTime() {
        return startTime;
    }

    public long retentionPeriodMs() {
        return retentionPeriodMs;
    }

    public static final class BuilderFlowable {

        private File directory;
        private String prefix;
        private long startTime;
        private long retentionPeriodMs = TimeUnit.DAYS.toMillis(7);

        BuilderFlowable() {
            // limit instantiation
        }

        public BuilderFlowable directory(File directory) {
            this.directory = directory;
            return this;
        }

        public BuilderFlowable prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public BuilderFlowable startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        public BuilderFlowable retentionPeriodHours(double retentionPeriodHours) {
            this.retentionPeriodMs = Math.round(retentionPeriodHours * TimeUnit.HOURS.toMillis(1));
            return this;
        }

        public <T> Flowable<T> serializer(Serializer<T> serializer) {
            Options options = new Options(directory, prefix, startTime, retentionPeriodMs);
            return new FlowablePersisting<T>(options, serializer);
        }

        public <T> Flowable<T> serializer(DataSerializer<T> ds) {
            return serializer(Serializers.from(ds));
        }

    }

    public static BuilderFlowable builderFlowable() {
        return new BuilderFlowable();
    }

}
