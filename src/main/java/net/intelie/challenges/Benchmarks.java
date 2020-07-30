package net.intelie.challenges;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Benchmarks {

    private void run(EventStore store, int writers, int readers) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(writers + readers);

        IntStream
                .rangeClosed(0, writers)
                .forEach(w -> {
                    executorService.execute(() -> {
                        IntStream
                                .rangeClosed(w*100, w*100 + 100)
                                .forEach(timestamp -> store.insert(new Event("type 1", timestamp)));
                    });
                });

        IntStream
                .rangeClosed(0, readers)
                .forEach(timestamp -> {
                    executorService.execute(() -> {
                        EventIterator eventIterator = store.query("type 1", 0, writers*100);
                        while (eventIterator.moveNext()) {
                            Event event = eventIterator.current();
                        }
                    });
                });

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Benchmark
    @Fork(value = 1, warmups = 0)
    @Warmup(iterations = 0)
    public void synch() throws InterruptedException {
        EventStore store = new EventStoreSynch();
        run(store, 10, 10);
    }

    @Benchmark
    @Fork(value = 1, warmups = 0)
    @Warmup(iterations = 0)
    public void synchWithFewWriters() throws InterruptedException {
        EventStore store = new EventStoreSynch();
        run(store, 2, 10);
    }

    @Benchmark
    @Fork(value = 1, warmups = 0)
    @Warmup(iterations = 0)
    public void rw() throws InterruptedException {
        EventStore store = new EventStoreRW();
        run(store, 10, 10);
    }

    @Benchmark
    @Fork(value = 1, warmups = 0)
    @Warmup(iterations = 0)
    public void rwWithFewWriters() throws InterruptedException {
        EventStore store = new EventStoreRW();
        run(store, 2, 10);
    }

    @Benchmark
    @Fork(value = 1, warmups = 0)
    @Warmup(iterations = 0)
    public void optimistic() throws InterruptedException {
        EventStore store = new EventStoreOptimistic();
        run(store, 10, 10);
    }

    @Benchmark
    @Fork(value = 1, warmups = 0)
    @Warmup(iterations = 0)
    public void optimmisticWithFewWriters() throws InterruptedException {
        EventStore store = new EventStoreOptimistic();
        run(store, 2, 10);
    }
}
