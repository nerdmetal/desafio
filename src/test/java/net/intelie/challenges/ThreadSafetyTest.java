package net.intelie.challenges;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class ThreadSafetyTest {
    private EventStore store;

    private void insertType1Events() {
        store.insert(new Event("type 1", 0L));
        store.insert(new Event("type 1", 1L));
        store.insert(new Event("type 1", 3L));
        store.insert(new Event("type 1", 5L));
        store.insert(new Event("type 1", 6L));
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void init() {
        store = new EventStoreOptimistic();
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

    private Runnable createProducer(CountDownLatch positioned, CountDownLatch modified, long... timestamps) {
        return () -> {
            try {
                positioned.await();
                Arrays.stream(timestamps)
                        .forEach((timestamp) -> {
                            Event event = new Event("type 1", timestamp);
                            store.insert(event);
                            System.out.printf("producer ins type: %s, timestamp: %d\n", event.type(), event.timestamp());
                        });
                modified.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
    }

    private Runnable createEraser(CountDownLatch positioned, CountDownLatch modified, long... timestamps) {
        long max = Arrays.stream(timestamps).max().getAsLong();
        return () -> {
            try {
                positioned.await();
                EventIterator eventIterator = store.query("type 1", 0, max + 1);
                while (eventIterator.moveNext()) {
                    Event event = eventIterator.current();
                    long i = Arrays.stream(timestamps)
                            .filter(e -> e == event.timestamp())
                            .findAny()
                            .orElse(-1);
                    if (i >= 0) {
                        eventIterator.remove();
                        System.out.printf("eraser del type: %s, timestamp: %d\n", event.type(), event.timestamp());
                    }
                }
                modified.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
    }

    private void runTestCase(long expected, long[] inserted, long[] removed) throws InterruptedException {
        insertType1Events();

        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch positioned = new CountDownLatch(1);
        CountDownLatch modified = new CountDownLatch(
                (inserted.length > 0 ? 1 : 0)
                        + (removed.length > 0 ? 1 : 0));

        Future<Long> consumer = executor.submit(() -> {
            EventIterator eventIterator = store.query("type 1", 0, 10);
            boolean verify = false;
            long result = -1;
            while (eventIterator.moveNext()) {
                Event event = eventIterator.current();
                System.out.printf("consumer read type: %s, timestamp: %d\n", event.type(), event.timestamp());

                if (verify) {
                    result = event.timestamp();
                    verify = false;
                }

                if (event.timestamp() == 3) {
                    positioned.countDown();
                    try {
                        modified.await();
                        verify = true;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        fail();
                    }
                }
            }

            return result;
        });

        if (inserted.length > 0) {
            Runnable producer = createProducer(positioned, modified, inserted);
            executor.execute(producer);
        }

        if (removed.length > 0) {
            Runnable eraser = createEraser(positioned, modified, removed);
            executor.execute(eraser);
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        assertEquals(0, positioned.getCount());
        assertEquals(0, modified.getCount());

        try {
            long received = consumer.get();
            assertEquals(expected, received);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void givenInitialData_whenInsert_4_thenTestConsumerUpdated() throws InterruptedException {
        runTestCase(4, new long[]{4}, new long[]{});
    }

    @Test
    public void givenInitialData_whenInsert_2_thenTestConsumerUpdated() throws InterruptedException {
        runTestCase(5, new long[]{2}, new long[]{});
    }

    @Test
    public void givenInitialData_whenRemove_3_thenTestConsumerUpdated() throws InterruptedException {
        runTestCase(5, new long[]{}, new long[]{3});
    }

    @Test
    public void givenInitialData_whenRemove_3_5_thenTestConsumerUpdated() throws InterruptedException {
        runTestCase(6, new long[]{}, new long[]{3, 5});
    }

    @Test
    public void givenInitialData_whenRemove_1_thenTestConsumerUpdated() throws InterruptedException {
        runTestCase(5, new long[]{}, new long[]{1});
    }

    @Test
    public void givenInitialData_whenRemove_1_3_thenTestConsumerUpdated() throws InterruptedException {
        runTestCase(5, new long[]{}, new long[]{1, 3});
    }

    @Test
    public void givenInitialData_whenRemove_0_1_thenTestConsumerUpdated() throws InterruptedException {
        runTestCase(5, new long[]{}, new long[]{0, 1});
    }

    @Test
    public void givenInitialData_whenInsert_4_Remove_3_thenTestConsumerUpdated() throws InterruptedException {
        runTestCase(4, new long[]{4}, new long[]{3});
    }

    @Test
    public void givenInitialData_whenInsert_4_Remove_1_thenTestConsumerUpdated() throws InterruptedException {
        runTestCase(4, new long[]{4}, new long[]{1});
    }

}