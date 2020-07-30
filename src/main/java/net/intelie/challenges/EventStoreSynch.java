package net.intelie.challenges;

import java.util.*;

/**
 * Thread-Safe EventStore.
 */
public class EventStoreSynch implements EventStore {

    private static final int INITIAL_CAPACITY = 1000;

    private Map<String, List<Event>> eventsByType = new HashMap<>();
    private final Object lock = new Object();

    @Override
    public void insert(Event event) {
        String type = event.type();
        synchronized (lock) {
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
    }

    @Override
    public void removeAll(String type) {
        synchronized (lock) {
            List<Event> events = this.eventsByType.get(type);
            if (events != null) {
                eventsByType.remove(type);
            }
        }
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        if (endTime <= startTime) {
            throw new IllegalArgumentException();
        }
        List<Event> events = this.eventsByType.get(type);
        return new EventIteratorSynch(lock, events, startTime, endTime);
    }
}
