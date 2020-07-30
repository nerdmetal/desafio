package net.intelie.challenges;

import java.util.*;

public class EventIteratorSynch implements EventIterator {
    private final Object lock;
    private List<Event> events;
    private long startTime;
    private long endTime;

    private Event current;
    private boolean started = false;
    private boolean eof = false;

    private int index;
    private long currentTimestamp;
    private long nextTimeStamp;

    public EventIteratorSynch(Object lock, List<Event> events, long startTime, long endTime) {
        this.lock = lock;
        this.events = events;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    private void updateCurrent(Event event) {
        current = event;
        currentTimestamp = event.timestamp();
        int i = index + 1;
        if (i < events.size()) {
            Event next = events.get(i);
            nextTimeStamp = next.timestamp();
        }
        else {
            nextTimeStamp = -1;
        }
    }

    private int findInsertPosition(long timestamp) {
        Event start = new Event("fake", timestamp);
        int i = Collections.binarySearch(events, start, Comparator.comparingLong(Event::timestamp));
        if (i < 0) {
            i = -i - 1;
        }

        return i;
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

            synchronized (lock) {
                index = findInsertPosition(startTime);

                if (index < events.size()) {
                    updateCurrent(events.get(index));
                    return true;
                }
                else {
                    eof = true;
                    return false;
                }
            }
        }

        if (index < events.size()) {
            Event check;
            synchronized (lock) {
                check = events.get(index);
            }
            if (check.timestamp() == currentTimestamp) {
                index++;
            }
            else {
                if (check.timestamp() < nextTimeStamp) {
                    int i = index;
                    synchronized (lock) {
                        while (i < events.size()) {
                            Event e = events.get(i);
                            if (e.timestamp() > currentTimestamp) {
                                break;
                            }
                            i++;
                        }
                    }
                    index = i;// + 1;
                }
                else if (check.timestamp() > nextTimeStamp) {
                    int i = index;
                    synchronized (lock) {
                        while (i >= 0) {
                            Event e = events.get(i);
                            if (e.timestamp() <= nextTimeStamp) {
                                if (e.timestamp() == nextTimeStamp) {
                                    index = i;
                                }
                                else {
                                    index = i + 1;
                                }
                                break;
                            }
                            i--;
                        }
                    }
                }
            }
        }

        if (index < events.size()) {
            synchronized (lock) {
                Event event = events.get(index);
                if (event.timestamp() < endTime) {
                    updateCurrent(event);
                    return true;
                }
            }
        }

        //TODO não percebe se todos os eventos do tipo foram removidos
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

        synchronized (lock) {
            events.remove(current);
        }
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
