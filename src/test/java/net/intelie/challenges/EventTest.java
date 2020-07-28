package net.intelie.challenges;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.*;

public class EventTest {
    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        //THIS IS A WARNING:
        //Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void moveNextWasNeverCalled_thenCurrent() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("moveNext() was never called");

        EventStore store = new EventStoreUnsafe();

        EventIterator eventIterator = store.query("none", 1, 2);
        eventIterator.current();
    }

    @Test
    public void moveNextWasNeverCalled_thenRemove() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("moveNext() was never called");

        EventStore store = new EventStoreUnsafe();

        EventIterator eventIterator = store.query("none", 1, 2);
        eventIterator.remove();
    }

    @Test
    public void givenEmptyStore_thenCurrentWasCalled() {
        EventStore store = new EventStoreUnsafe();

        EventIterator eventIterator = store.query("none", 1, 2);
        assertFalse(eventIterator.moveNext());

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("no more events");
        eventIterator.current();
    }

    @Test
    public void givenEmptyStore_thenRemoveWasCalled() {
        EventStore store = new EventStoreUnsafe();

        EventIterator eventIterator = store.query("none", 1, 2);
        assertFalse(eventIterator.moveNext());

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("no more events");
        eventIterator.remove();
    }

    @Test
    public void inverseQuery() {
        EventStore store = new EventStoreUnsafe();

        store.insert(new Event("type 1", 1));
        store.insert(new Event("type 1", 3));
        store.insert(new Event("type 1", 5));

        EventIterator eventIterator = store.query("type 1", 2, 1);
        exceptionRule.expect(IllegalArgumentException.class);
        eventIterator.moveNext();
    }

    @Test
    public void iterations() {
        EventStore store = new EventStoreUnsafe();

        store.insert(new Event("type 1", 1));
        store.insert(new Event("type 1", 3));
        store.insert(new Event("type 1", 5));

        EventIterator eventIterator = store.query("none", 1, 5);
        assertFalse(eventIterator.moveNext());

        eventIterator = store.query("type 1", 1, 5);
        int count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            count++;
        }
        assertEquals(2, count);

        eventIterator = store.query("type 1", 1, 6);
        count = 0;
        while (eventIterator.moveNext()) {
            Event event = eventIterator.current();
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void moreTypes() {
        EventStore store = new EventStoreUnsafe();

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
        EventStore store = new EventStoreUnsafe();

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
        EventStore store = new EventStoreUnsafe();

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