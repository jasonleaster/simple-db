package simpledb.page.btree;

import simpledb.BufferPool;
import simpledb.Catalog;
import simpledb.Database;
import simpledb.Debug;
import simpledb.Type;
import simpledb.dbfile.BTreeFile;
import simpledb.exception.DbException;
import simpledb.field.Field;
import simpledb.field.IntField;
import simpledb.operator.Predicate;
import simpledb.page.pageid.BTreePageId;
import simpledb.tuple.RecordId;
import simpledb.tuple.Tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Each instance of BTreeLeafPage stores data for one page of a BTreeFile and
 * implements the Page interface that is used by BufferPool.
 *
 * @see BTreeFile
 * @see BufferPool
 */
public class BTreeLeafPage extends BTreePage {
    private final byte header[];
    private final Tuple tuples[];
    private final int numSlots;

    /**
     * This int filed is page number
     */
    private int leftSibling; // leaf node or 0
    private int rightSibling; // leaf node or 0

    /**
     * Create a BTreeLeafPage from a set of bytes of data read from disk.
     * The format of a BTreeLeafPage is a set of header bytes indicating
     * the slots of the page that are in use, and some number of tuple slots,
     * as well as some extra bytes for the parent and sibling pointers.
     * Specifically, the number of tuples is equal to: <predicteOper>
     * floor((BufferPool.getPageSize()*8 - extra bytes*8) / (tuple size * 8 + 1))
     * <predicteOper> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <predicteOper>
     * ceiling(no. tuple slots / 8)
     * <predicteOper>
     *
     * @param id   - the id of this page
     * @param data - the raw data of this page
     * @param key  - the field which the index is keyed on
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public BTreeLeafPage(BTreePageId id, byte[] data, int key) throws IOException {
        super(id, key);
        this.numSlots = getMaxTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // Read the parent and sibling pointers
        try {
            Field f = Type.INT_TYPE.parse(dis);
            this.parent = ((IntField) f).getValue();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }

        try {
            Field f = Type.INT_TYPE.parse(dis);
            this.leftSibling = ((IntField) f).getValue();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }

        try {
            Field f = Type.INT_TYPE.parse(dis);
            this.rightSibling = ((IntField) f).getValue();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++) {
            header[i] = dis.readByte();
        }

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++) {
                tuples[i] = readNextTuple(dis, i);
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the maximum number of tuples this page can hold.
     */
    public int getMaxTuples() {
        int bitsPerTupleIncludingHeader = td.getSize() * 8 + 1;
        // extraBits are: left sibling pointer, right sibling pointer, parent pointer
        int extraBits = 3 * INDEX_SIZE * 8;
        int tuplesPerPage = (BufferPool.getPageSize() * 8 - extraBits) / bitsPerTupleIncludingHeader; //round down
        return tuplesPerPage;
    }

    /**
     * Computes the number of bytes in the header of a page in a BTreeFile with
     * each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        int tuplesPerPage = getMaxTuples();
        int headerBytes = (tuplesPerPage / 8);
        if (headerBytes * 8 < tuplesPerPage) {
            headerBytes++;
        }
        return headerBytes;
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public BTreeLeafPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new BTreeLeafPage(pid, oldDataRef, keyField);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * Read tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <predicteOper>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the BTreeLeafPage constructor and
     * have it produce an identical BTreeLeafPage object.
     *
     * @return A byte array corresponding to the bytes of this page.
     * @see #BTreeLeafPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // write out the parent and sibling pointers
        try {
            dos.writeInt(parent);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            dos.writeInt(leftSibling);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            dos.writeInt(rightSibling);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // create the header of the page
        for (int i = 0; i < header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn'tableId happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zeroLen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length + 3 * INDEX_SIZE); //- numSlots * tupleDesc.getSize();
        byte[] zeroes = new byte[zeroLen];
        try {
            dos.write(zeroes, 0, zeroLen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        RecordId rid = t.getRecordId();
        if (rid == null)
            throw new DbException("tried to delete tuple with null rid");
        if ((rid.getPageId().getPageNumber() != pid.getPageNumber()) || (rid.getPageId().getTableId() != pid.getTableId()))
            throw new DbException("tried to delete tuple on invalid page or table");
        if (!isSlotUsed(rid.getTupleNumber()))
            throw new DbException("tried to delete null tuple.");
        markSlotUsed(rid.getTupleNumber(), false);
        t.setRecordId(null);
    }

    /**
     * Adds the specified tuple to the page such that all records remain in sorted order;
     * the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        if (!t.getTupleDesc().equals(td))
            throw new DbException("type mismatch, in addTuple");

        // find the first empty slot
        int emptySlot = -1;
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                emptySlot = i;
                break;
            }
        }

        if (emptySlot == -1)
            throw new DbException("called addTuple on page with no empty slots.");

        // find the last key less than or equal to the key being inserted
        int lessOrEqKey = -1;
        Field key = t.getField(keyField);
        for (int i = 0; i < numSlots; i++) {
            if (isSlotUsed(i)) {
                if (tuples[i].getField(keyField).compare(Predicate.Op.LESS_THAN_OR_EQ, key))
                    lessOrEqKey = i;
                else
                    break;
            }
        }

        // shift records back or forward to fill empty slot and make room for new record
        // while keeping records in sorted order
        int goodSlot = -1;
        if (emptySlot < lessOrEqKey) {
            for (int i = emptySlot; i < lessOrEqKey; i++) {
                moveRecord(i + 1, i);
            }
            goodSlot = lessOrEqKey;
        } else {
            for (int i = emptySlot; i > lessOrEqKey + 1; i--) {
                moveRecord(i - 1, i);
            }
            goodSlot = lessOrEqKey + 1;
        }

        // insert new record into the correct spot in sorted order
        markSlotUsed(goodSlot, true);
        Debug.log(1, "BTreeLeafPage.insertTuple: new tuple, tableId = %d pageId = %d slotId = %d", pid.getTableId(), pid.getPageNumber(), goodSlot);
        RecordId rid = new RecordId(pid, goodSlot);
        t.setRecordId(rid);
        tuples[goodSlot] = t;
    }

    /**
     * Move a record from one slot to another slot, and update the corresponding
     * headers and RecordId
     */
    private void moveRecord(int from, int to) {
        if (!isSlotUsed(to) && isSlotUsed(from)) {
            markSlotUsed(to, true);
            RecordId rid = new RecordId(pid, to);
            tuples[to] = tuples[from];
            tuples[to].setRecordId(rid);
            markSlotUsed(from, false);
        }
    }

    /**
     * Get the id of the left sibling of this page
     *
     * @return the id of the left sibling
     */
    public BTreePageId getLeftSiblingId() {
        if (leftSibling == 0) {
            return null;
        }
        return new BTreePageId(pid.getTableId(), leftSibling, BTreePageId.LEAF);
    }

    /**
     * Get the id of the right sibling of this page
     *
     * @return the id of the right sibling
     */
    public BTreePageId getRightSiblingId() {
        if (rightSibling == 0) {
            return null;
        }
        return new BTreePageId(pid.getTableId(), rightSibling, BTreePageId.LEAF);
    }

    /**
     * Set the left sibling id of this page
     *
     * @param id - the new left sibling id
     * @throws DbException if the id is not valid
     */
    public void setLeftSiblingId(BTreePageId id) throws DbException {
        if (id == null) {
            leftSibling = 0;
        } else {
            if (id.getTableId() != pid.getTableId()) {
                throw new DbException("table id mismatch in setLeftSiblingId");
            }
            if (id.pgcateg() != BTreePageId.LEAF) {
                throw new DbException("leftSibling must be a leaf node");
            }
            leftSibling = id.getPageNumber();
        }
    }

    /**
     * Set the right sibling id of this page
     *
     * @param id - the new right sibling id
     * @throws DbException if the id is not valid
     */
    public void setRightSiblingId(BTreePageId id) throws DbException {
        if (id == null) {
            rightSibling = 0;
        } else {
            if (id.getTableId() != pid.getTableId()) {
                throw new DbException("table id mismatch in setRightSiblingId");
            }
            if (id.pgcateg() != BTreePageId.LEAF) {
                throw new DbException("rightSibling must be a leaf node");
            }
            rightSibling = id.getPageNumber();
        }
    }

    /**
     * Returns the number of tuples currently stored on this page
     */
    public int getNumTuples() {
        return numSlots - getNumEmptySlots();
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int cnt = 0;
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                cnt++;
            }
        }
        return cnt;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    @Override
    public boolean isSlotUsed(int i) {
        int headerBit = i % 8;
        int headerByte = (i - headerBit) / 8;
        return (header[headerByte] & (1 << headerBit)) != 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        int headerBit = i % 8;
        int headerByte = (i - headerBit) / 8;

        Debug.log(1, "BTreeLeafPage.setSlot: setting slot %d to %b", i, value);
        if (value) {
            header[headerByte] |= 1 << headerBit;
        }
        else {
            header[headerByte] &= (0xFF ^ (1 << headerBit));
        }

    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn'tableId return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        return new BTreeLeafPageIterator(this);
    }

    /**
     * @return a reverse iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn'tableId return tuples in empty slots!)
     */
    public Iterator<Tuple> reverseIterator() {
        return new BTreeLeafPageReverseIterator(this);
    }

    /**
     * protected method used by the iterator to get the ith tuple out of this page
     *
     * @param i - the index of the tuple
     * @return the ith tuple in the page
     * @throws NoSuchElementException
     */
    Tuple getTuple(int i) throws NoSuchElementException {

        if (i >= tuples.length)
            throw new NoSuchElementException();

        try {
            if (!isSlotUsed(i)) {
                Debug.log(1, "BTreeLeafPage.getTuple: slot %d in %d:%d is not used", i, pid.getTableId(), pid.getPageNumber());
                return null;
            }

            Debug.log(1, "BTreeLeafPage.getTuple: returning tuple %d", i);
            return tuples[i];

        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
    }

    public void checkRep(int fieldId, Field lowerBound, Field upperBound, boolean checkOccupancy, int depth) {
        Field prev = lowerBound;
        assert (this.getId().pgcateg() == BTreePageId.LEAF);

        Iterator<Tuple> it = this.iterator();
        while (it.hasNext()) {
            Tuple t = it.next();
            assert (null == prev || prev.compare(Predicate.Op.LESS_THAN_OR_EQ, t.getField(fieldId)));
            prev = t.getField(fieldId);
            assert (t.getRecordId().getPageId().equals(this.getId()));
        }

        if (null != upperBound && null != prev) {
            assert (prev.compare(Predicate.Op.LESS_THAN_OR_EQ, upperBound));
        }

        if (checkOccupancy && depth > 0) {
            assert (getNumTuples() >= getMaxTuples() / 2);
        }
    }


    /**
     * Helper class that implements the Java Iterator for tuples on a BTreeLeafPage.
     */
    private class BTreeLeafPageIterator implements Iterator<Tuple> {
        int curTuple = 0;
        Tuple nextToReturn = null;
        BTreeLeafPage p;

        public BTreeLeafPageIterator(BTreeLeafPage p) {
            this.p = p;
        }

        @Override
        public boolean hasNext() {
            if (nextToReturn != null) {
                return true;
            }

            try {
                while (true) {
                    nextToReturn = p.getTuple(curTuple++);
                    if (nextToReturn != null) {
                        return true;
                    }
                }
            } catch (NoSuchElementException e) {
                return false;
            }
        }

        @Override
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

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Helper class that implements the Java Iterator for tuples on a BTreeLeafPage in reverse.
     */
    private class BTreeLeafPageReverseIterator implements Iterator<Tuple> {
        int curTuple;
        Tuple nextToReturn = null;
        BTreeLeafPage p;

        public BTreeLeafPageReverseIterator(BTreeLeafPage p) {
            this.p = p;
            this.curTuple = p.getMaxTuples() - 1;
        }

        @Override
        public boolean hasNext() {
            if (nextToReturn != null) {
                return true;
            }


            try {
                while (curTuple >= 0) {
                    nextToReturn = p.getTuple(curTuple--);
                    if (nextToReturn != null) {
                        return true;
                    }
                }
                return false;
            } catch (NoSuchElementException e) {
                return false;
            }
        }

        @Override
        public Tuple next() {
            Tuple next = nextToReturn;

            if (next == null) {
                if (hasNext()) {
                    next = nextToReturn;
                    nextToReturn = null;
                    return next;
                } else {
                    throw new NoSuchElementException();
                }
            } else {
                nextToReturn = null;
                return next;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}