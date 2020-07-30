package net.intelie.challenges;

import net.intelie.challenges.exploration.EventStoreOptimistic;
import net.intelie.challenges.exploration.EventStoreRW;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Non-deterministic tests only to provide some evidence of thread safety.
 */
public class ThreadSafetyTest {
    private EventStore store;

    @Before
    public void init() {
        store = new EventStoreSynch();
    }

    @Test
    public void testInsertThreadSafety() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        executorService.execute(() -> store.insert(new Event("type 1", 1)));
        executorService.execute(() -> store.insert(new Event("type 1", 2)));
        executorService.execute(() -> store.insert(new Event("type 1", 3)));

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);

        EventIterator eventIterator = store.query("type 1", 1, 10);
        int count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.printf("read type: %s, timestamp: %d\n", event.type(), event.timestamp());
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void givenProducersAndConsumers_thenTestRemoveThreadSafety() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        IntStream
                .rangeClosed(0, 10)
                .forEach(timestamp -> store.insert(new Event("type 1", timestamp)));

        Runnable producer = () -> IntStream
                .rangeClosed(11, 100)
                .forEach(timestamp -> {
                    Event event = new Event("type 1", timestamp);
                    store.insert(event);
                    System.out.printf("ins type: %s, timestamp: %d\n", event.type(), event.timestamp());
                });

        executorService.execute(producer);

        executorService.execute(() -> {
            EventIterator eventIterator = store.query("type 1", 0, 20);
            while (eventIterator.moveNext()) {
                Event event = eventIterator.current();
                System.out.printf("th1 read type: %s, timestamp: %d\n", event.type(), event.timestamp());
                if (event.timestamp() == 3L) {
                    System.out.printf("del1 type: %s, timestamp: %d\n", event.type(), event.timestamp());
                    eventIterator.remove();
                }
            }
        });

        executorService.execute(() -> {
            EventIterator eventIterator = store.query("type 1", 0, 20);
            while (eventIterator.moveNext()) {
                Event event = eventIterator.current();
                System.out.printf("th2 read type: %s, timestamp: %d\n", event.type(), event.timestamp());
                if (event.timestamp() == 5L) {
                    System.out.printf("del2 type: %s, timestamp: %d\n", event.type(), event.timestamp());
                    eventIterator.remove();
                }
            }
        });

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        EventIterator eventIterator = store.query("type 1", 1, 10);
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.printf("read type: %s, timestamp: %d\n", event.type(), event.timestamp());
            assertTrue(event.timestamp() != 3 && event.timestamp() != 5);
        }
    }

}