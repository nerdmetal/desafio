package net.intelie.challenges.exploration;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Lock;

public class EventIteratorRW implements EventIterator {
    private final Lock readLock;
    private final Lock writeLock;

    private List<Event> events;
    private long startTime;
    private long endTime;

    private Event current;
    private boolean started = false;
    private boolean eof = false;

    private int index;
    private long currentTimestamp;
    private long nextTimeStamp;

    public EventIteratorRW(Lock readLock, Lock writeLock, List<Event> events, long startTime, long endTime) {
        this.readLock = readLock;
        this.writeLock = writeLock;
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

            readLock.lock();
            try {
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
            finally {
                readLock.unlock();
            }
        }

        if (index < events.size()) {
            Event check;
            readLock.lock();
            try {
                check = events.get(index);
            }
            finally {
                readLock.unlock();
            }
            if (check.timestamp() == currentTimestamp) {
                index++;
            }
            else {
                if (check.timestamp() < nextTimeStamp) {
                    int i = index;
                    readLock.lock();
                    try {
                        while (i < events.size()) {
                            Event e = events.get(i);
                            if (e.timestamp() > currentTimestamp) {
                                break;
                            }
                            i++;
                        }
                    }
                    finally {
                        readLock.unlock();
                    }
                    index = i;// + 1;
                }
                else if (check.timestamp() > nextTimeStamp) {
                    int i = index;
                    readLock.lock();
                    try {
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
                    finally {
                        readLock.unlock();
                    }
                }
            }
        }

        if (index < events.size()) {
            readLock.lock();
            try {
                Event event = events.get(index);
                if (event.timestamp() < endTime) {
                    updateCurrent(event);
                    return true;
                }
            }
            finally {
                readLock.unlock();
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

        writeLock.lock();
        try {
            events.remove(current);
        }
        finally {
            writeLock.unlock();
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
