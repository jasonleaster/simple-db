package simpledb.transaction;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId implements Serializable {

    private static final long serialVersionUID = 1L;

    private static AtomicLong counter = new AtomicLong(0);
    private final long myid;
    private final long startTimestamp;

    public TransactionId() {
        myid = counter.getAndIncrement();
        startTimestamp = System.currentTimeMillis();
    }

    public long getId() {
        return myid;
    }

    @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TransactionId other = (TransactionId) obj;
		if (myid != other.myid)
			return false;
		return true;
	}

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (myid ^ (myid >>> 32));
		return result;
	}

    public long getStartTimestamp() {
        return startTimestamp;
    }
}
