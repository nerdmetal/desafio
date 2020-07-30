package net.intelie.challenges.exploration;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.challenges.EventStore;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EventStoreRW implements EventStore {

    private static final int INITIAL_CAPACITY = 1000;

    private Map<String, List<Event>> eventsByType = new HashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    @Override
    public void insert(Event event) {
        String type = event.type();
        writeLock.lock();
        try {
            List<Event> events = this.eventsByType.get(type);
            if (events == null) {
                events = new ArrayList<>(INITIAL_CAPACITY);
                eventsByType.put(type, events);
                events.add(event);
            }
            else {
                int index = Collections.binarySearch(events, event, Comparator.comparingLong(Event::timestamp));
                if (index < 0) {
                    index = -index - 1;
                }
                events.add(index, event);
            }
        }
        finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeAll(String type) {
        writeLock.lock();
        try {
            List<Event> events = this.eventsByType.get(type);
            if (events != null) {
                eventsByType.remove(type);
            }
        }
        finally {
            writeLock.unlock();
        }
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        if (endTime <= startTime) {
            throw new IllegalArgumentException();
        }
        List<Event> events = this.eventsByType.get(type);
        return new EventIteratorRW(readLock, writeLock, events, startTime, endTime);
    }
}
