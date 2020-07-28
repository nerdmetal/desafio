package net.intelie.challenges;

import java.util.*;

public class EventStoreUnsafe implements EventStore {

    private Map<String, SortedSet<Event>> eventsByType = new HashMap<>();

    @Override
    public void insert(Event event) {
        String type = event.type();
        SortedSet<Event> events = this.eventsByType.get(type);
        if (events == null) {
            events = new TreeSet<>(Comparator.comparingLong(Event::timestamp)); //TODO avaliar conversão de long para Long
            eventsByType.put(type, events);
        }

        events.add(event);
    }

    @Override
    public void removeAll(String type) {
        SortedSet<Event> events = this.eventsByType.get(type);
        if (events != null) {
            eventsByType.remove(type);
        }
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        SortedSet<Event> events = this.eventsByType.get(type);
        return new EventIteratorUnsafe(events, startTime, endTime);
    }
}
