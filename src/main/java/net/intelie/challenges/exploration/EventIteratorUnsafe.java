package net.intelie.challenges.exploration;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;

import java.util.Iterator;
import java.util.SortedSet;

public class EventIteratorUnsafe implements EventIterator {
    private SortedSet<Event> events;
    private long startTime;
    private long endTime;
    private Event current;
    private boolean started = false;
    private boolean eof = false;
    private Iterator<Event> iterator;

    public EventIteratorUnsafe(SortedSet<Event> events, long startTime, long endTime) {
        this.events = events;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public boolean moveNext() {
        if (events == null) {
            started = true;
            eof = true;
            return false;
        }

        if (!started) {
            started = true;

            //TODO melhorar
            Event start = new Event("fake", startTime);
            Event end = new Event("fake", endTime);
            iterator = this.events.subSet(start, end).iterator();
        }

        if (iterator.hasNext()) {
            current = iterator.next();
            return true;
        }

        eof = true;
        return false;
    }

    @Override
    public Event current() {
        checkConditions();

        return current;
    }

    @Override
    public void remove() {
        checkConditions();

        iterator.remove();
    }

    @Override
    public void close() throws Exception {

    }

    private void checkConditions() {
        if (!started) {
            throw new IllegalStateException("moveNext() was never called");
        }

        if (eof) {
            throw new IllegalStateException("no more events");
        }
    }

}
