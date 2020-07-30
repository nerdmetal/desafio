package net.intelie.challenges;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

public class EventStoreOptimistic implements EventStore {

    private static final int INITIAL_CAPACITY = 1000;

    private Map<String, List<Event>> eventsByType = new HashMap<>();
    private final StampedLock lock = new StampedLock();

    @Override
    public void insert(Event event) {
        String type = event.type();
        long stamp = lock.writeLock();
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
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void removeAll(String type) {
        long stamp = lock.writeLock();
        try {
            List<Event> events = this.eventsByType.get(type);
            if (events != null) {
                eventsByType.remove(type);
            }
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        if (endTime <= startTime) {
            throw new IllegalArgumentException();
        }
        List<Event> events = this.eventsByType.get(type);
        return new EventIteratorOptimistic(lock, events, startTime, endTime);
    }
}
