package simpledb;

import java.util.*;
import java.io.*;


class HeapPageIterator implements Iterator<Tuple> {
    int curTuple = 0;
    Tuple nextToReturn = null;
    HeapPage p;

    public HeapPageIterator(HeapPage p) {
        this.p = p;
    }

    public boolean hasNext() {
        if (nextToReturn != null)
            return true;

        try {
            while (true) {
                nextToReturn = p.getTuple(curTuple++);
                if(nextToReturn != null)
                    return true;
            }
        } catch(NoSuchElementException e) {
            return false;
        }
    }

    public Tuple next() {
        Tuple next = nextToReturn;

        if (next == null) {
            if (hasNext()) {
                next = nextToReturn;
                nextToReturn = null;
                return next;
            } else
                throw new NoSuchElementException();
        } else {
            nextToReturn = null;
            return next;
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
