package net.intelie.challenges;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class EventIteratorTest {
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
        store = new EventStoreSynch();
    }

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
        exceptionRule.expect(IllegalArgumentException.class);
        EventIterator eventIterator = store.query("type 1", 2, 1);
        eventIterator.moveNext();
    }

    @Test
    public void testClose() {
        try (EventIterator eventIterator = store.query("type 1", 0, 1);) {
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testEmptyStore() {
        insertType1Events();

        EventIterator eventIterator = store.query("none", 1, 5);
        assertFalse(eventIterator.moveNext());
    }

    @Test
    public void testEndTime() {
        insertType1Events();
        EventIterator eventIterator = store.query("type 1", 0, 5);
        int count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.printf("read type: %s, timestamp: %d\n", event.type(), event.timestamp());
            assertTrue(event.timestamp() >= 0);
            assertTrue(event.timestamp() < 5);
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void testStartTime() {
        insertType1Events();

        EventIterator eventIterator = store.query("type 1", 3, 10);
        int count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.printf("read type: %s, timestamp: %d\n", event.type(), event.timestamp());
            assertTrue(event.timestamp() >= 3);
            assertTrue(event.timestamp() < 10);
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void testEmptyTimeRange() {
        insertType1Events();

        EventIterator eventIterator = store.query("type 1", 10, 20);
        assertFalse(eventIterator.moveNext());
    }

    @Test
    public void testMoreTypes() {
        store.insert(new Event("type 1", 1));
        store.insert(new Event("type 1", 3));
        store.insert(new Event("type 1", 5));

        store.insert(new Event("type 2", 2));
        store.insert(new Event("type 2", 4));

        EventIterator eventIterator = store.query("type 1", 1, 10);
        int count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            assertEquals("type 1", event.type());
            count++;
        }
        assertEquals(3, count);

        eventIterator = store.query("type 2", 1, 10);
        count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            assertEquals("type 2", event.type());
            count++;
        }
        assertEquals(2, count);
    }

    @Test
    public void givenInitialData_whenInsertionBefore_testConsumerUpdated() {
        insertType1Events();

        EventIterator eventIterator = store.query("type 1", 3, 10);
        int count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.printf("read type: %s, timestamp: %d\n", event.type(), event.timestamp());
            assertTrue(event.timestamp() >= 3);
            assertTrue(event.timestamp() < 10);
            count++;
        }
        assertEquals(3, count);
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

}