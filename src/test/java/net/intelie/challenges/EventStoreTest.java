package net.intelie.challenges;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class EventStoreTest {
    private EventStore store;

    @Before
    public void init() {
        store = new EventStoreSynch();
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void moveNextWasNeverCalled_thenCurrent() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("moveNext() was never called");

        EventIterator eventIterator = store.query("none", 1, 2);
        eventIterator.current();
    }

    @Test
    public void moveNextWasNeverCalled_thenRemove() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("moveNext() was never called");

        EventIterator eventIterator = store.query("none", 1, 2);
        eventIterator.remove();
    }

    @Test
    public void givenEmptyStore_thenCurrentWasCalled() {
        EventIterator eventIterator = store.query("none", 1, 2);
        assertFalse(eventIterator.moveNext());

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("no more events");
        eventIterator.current();
    }

    @Test
    public void givenEmptyStore_thenRemoveWasCalled() {
        EventIterator eventIterator = store.query("none", 1, 2);
        assertFalse(eventIterator.moveNext());

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("no more events");
        eventIterator.remove();
    }

    @Test
    public void inverseQuery() {
        store.insert(new Event("type 1", 1));
        store.insert(new Event("type 1", 3));
        store.insert(new Event("type 1", 5));

        exceptionRule.expect(IllegalArgumentException.class);
        EventIterator eventIterator = store.query("type 1", 2, 1);
        eventIterator.moveNext();
    }

    @Test
    public void iterations() {
        store.insert(new Event("type 1", 1));
        store.insert(new Event("type 1", 3));
        store.insert(new Event("type 1", 5));

        EventIterator eventIterator = store.query("none", 1, 5);
        assertFalse(eventIterator.moveNext());

        eventIterator = store.query("type 1", 1, 5);
        int count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            assertTrue(event.timestamp() >= 1);
            assertTrue(event.timestamp() < 5);
            count++;
        }
        assertEquals(2, count);

        eventIterator = store.query("type 1", 1, 6);
        count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            assertTrue(event.timestamp() >= 1);
            assertTrue(event.timestamp() < 6);
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void moreTypes() {
        store.insert(new Event("type 1", 1));
        store.insert(new Event("type 1", 3));
        store.insert(new Event("type 1", 5));

        store.insert(new Event("type 2", 2));
        store.insert(new Event("type 2", 4));

        EventIterator eventIterator = store.query("type 1", 1, 10);
        int count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            count++;
        }
        assertEquals(3, count);

        eventIterator = store.query("type 2", 1, 10);
        count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            count++;
        }
        assertEquals(2, count);
    }

    @Test
    public void remove() {
        store.insert(new Event("type 1", 1));
        store.insert(new Event("type 1", 3));
        store.insert(new Event("type 1", 5));

        EventIterator eventIterator = store.query("type 1", 1, 10);
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            if (event.timestamp() == 3L) {
                eventIterator.remove();
            }
        }

        eventIterator = store.query("type 1", 1, 10);
        int count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            assertNotEquals(3, event.timestamp());
            count++;
        }
        assertEquals(2, count);
    }

    @Test
    public void removeAll() {
        store.insert(new Event("type 1", 1));
        store.insert(new Event("type 1", 3));
        store.insert(new Event("type 1", 5));

        store.insert(new Event("type 2", 2));
        store.insert(new Event("type 2", 4));

        store.removeAll("type 1");

        EventIterator eventIterator = store.query("type 1", 1, 10);
        assertFalse(eventIterator.moveNext());

        eventIterator = store.query("type 2", 1, 10);
        int count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            count++;
        }
        assertEquals(2, count);
    }

    @Test
    public void testInsertThreadSafety() {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        executorService.execute(() -> store.insert(new Event("type 1", 1)));
        executorService.execute(() -> store.insert(new Event("type 1", 2)));
        executorService.execute(() -> store.insert(new Event("type 1", 3)));

        executorService.shutdown();

        try {
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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
    public void testRemoveThreadSafety() {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        IntStream
                .rangeClosed(0, 10)
                .forEach(timestamp -> {
                    store.insert(new Event("type 1", timestamp));
                    System.out.printf("ins timestamp: %d\n", timestamp);
                });

        Runnable producer = () -> IntStream
                .rangeClosed(11, 100)
                .forEach(timestamp -> {
                    store.insert(new Event("type 1", timestamp));
                    System.out.printf("ins timestamp: %d\n", timestamp);
                });

        executorService.execute(producer);

        executorService.execute(() -> {
            EventIterator eventIterator = store.query("type 1", 0, 10);
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
            EventIterator eventIterator = store.query("type 1", 0, 10);
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
        try {
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        EventIterator eventIterator = store.query("type 1", 1, 10);
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.printf("read type: %s, timestamp: %d\n", event.type(), event.timestamp());
            assertTrue(event.timestamp() != 3 && event.timestamp() != 5);
        }
    }

}