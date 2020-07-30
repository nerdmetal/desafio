package net.intelie.challenges;

import net.intelie.challenges.exploration.EventStoreOptimistic;
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
    public void testRemoveAll() {
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

}