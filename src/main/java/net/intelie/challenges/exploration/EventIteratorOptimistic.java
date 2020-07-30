package net.intelie.challenges.exploration;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

public class EventIteratorOptimistic implements EventIterator {
    private final StampedLock lock;

    private List<Event> events;
    private long startTime;
    private long endTime;

    private Event current;
    private boolean started = false;
    private boolean eof = false;

    private int index;
    private long currentTimestamp;
    private long nextTimeStamp;

    public EventIteratorOptimistic(StampedLock lock, List<Event> events, long startTime, long endTime) {
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

    private int findCurrentTimestamp() {
        int i = index;
        while (i < events.size()) {
            Event e = events.get(i);
            if (e.timestamp() > currentTimestamp) {
                break;
            }
            i++;
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

            long findStamp = lock.tryOptimisticRead();
            index = findInsertPosition(startTime);
            if (!lock.validate(findStamp)) {
                findStamp = lock.readLock();
                try {
                    index = findInsertPosition(startTime);
                } finally {
                    lock.unlockRead(findStamp);
                }
            }

            if (index < events.size()) {
                long stamp = lock.tryOptimisticRead();
                updateCurrent(events.get(index));
                if (!lock.validate(stamp)) {
                    stamp = lock.readLock();
                    try {
                        updateCurrent(events.get(index));
                    } finally {
                        lock.unlockRead(stamp);
                    }
                }
                return true;
            }
            else {
                eof = true;
                return false;
            }
        }

        if (index < events.size()) {
            Event check;
            long stamp = lock.tryOptimisticRead();
            check = events.get(index);
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    check = events.get(index);
                } finally {
                    lock.unlockRead(stamp);
                }
            }
            if (check.timestamp() == currentTimestamp) {
                index++;
            }
            else {
                if (check.timestamp() < nextTimeStamp) {
                    long forwardStamp = lock.tryOptimisticRead();
                    index = findCurrentTimestamp();
                    if (!lock.validate(forwardStamp)) {
                        forwardStamp = lock.readLock();
                        try {
                            index = findCurrentTimestamp();
                        } finally {
                            lock.unlockRead(forwardStamp);
                        }
                    }
                }
                else if (check.timestamp() > nextTimeStamp) {
                    int i = index;
                    long readStamp = lock.readLock();
                    try {
                        //TODO criar método para poder usar lock otimista
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
                        lock.unlockRead(readStamp);
                    }
                }
            }
        }

        if (index < events.size()) {
            long stamp = lock.tryOptimisticRead();
            Event event = events.get(index);
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    event = events.get(index);
                }
                finally {
                    lock.unlockRead(stamp);
                }
            }
            if (event.timestamp() < endTime) {
                long updateCurrentStamp = lock.tryOptimisticRead();
                updateCurrent(event);
                if (!lock.validate(updateCurrentStamp)) {
                    updateCurrentStamp = lock.readLock();
                    try {
                        updateCurrent(event);
                    }
                    finally {
                        lock.unlockRead(updateCurrentStamp);
                    }
                }

                return true;
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

        long stamp = lock.writeLock();
        try {
            events.remove(current);
        }
        finally {
            lock.unlockWrite(stamp);
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
