package simpledb.page;

import simpledb.BufferPool;
import simpledb.Catalog;
import simpledb.Database;
import simpledb.exception.DbException;
import simpledb.tuple.RecordId;
import simpledb.transaction.TransactionId;
import simpledb.dbfile.HeapFile;
import simpledb.field.Field;
import simpledb.page.pageid.HeapPageId;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    private static final int SLOT_UNUSED = 0;
    private static final int BYTE_SIZE = 8;

    final private HeapPageId pageId;
    final private TupleDesc tupleDesc;
    final private byte[] header;
    final private Tuple[] tuples;

    private TransactionId dirtyTransaction;

    private byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <predicteOper>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <predicteOper> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <predicteOper>
     * ceiling(no. tuple slots / 8)
     * <predicteOper>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pageId = id;
        this.tupleDesc = Database.getCatalog().getTupleDesc(id.getTableId());
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++) {
            header[i] = dis.readByte();
        }

        tuples = new Tuple[getNumTuples()];
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
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // 8 bit
        final int byteSize = 8;
        // 1 bit
        final int slotSize = 1;

        return (BufferPool.getPageSize() * byteSize) / (tupleDesc.getSize() * byteSize + slotSize);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        return (getNumTuples() + 7 ) / 8 ;
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    @Override
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pageId, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    @Override
    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    @Override
    public HeapPageId getId() {
        return this.pageId;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < tupleDesc.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(tupleDesc);
        RecordId rid = new RecordId(pageId, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < tupleDesc.numFields(); j++) {
                Field f = tupleDesc.getFieldType(j).parse(dis);
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
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    @Override
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

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
                for (int j = 0; j < tupleDesc.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < tupleDesc.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zeroLen = BufferPool.getPageSize() - (header.length + tupleDesc.getSize() * tuples.length);
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
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        //all 0
        return new byte[BufferPool.getPageSize()];
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param tuple The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple tuple) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (tuple == null) {
            return;
        }

        boolean findTarget = false;
        for (int i = 0; i < this.getNumTuples(); i++) {
            if (tuple.equals(this.tuples[i])) {
                markSlotUsed(i, false);
                this.tuples[i] = null;
                findTarget = true;
            }
        }
        if (!findTarget) {
            throw new DbException("this tuple is not on this page," +
                    " or tuple slot is already empty.");
        }
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param tuple The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple tuple) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (!this.tupleDesc.equals(tuple.getTupleDesc())) {
            return;
        }

        for(int i = 0; i < this.getNumTuples(); i++) {
            if (!this.isSlotUsed(i)) {
                markSlotUsed(i, true);
                tuple.setRecordId(new RecordId(this.pageId, this.getNumTuples() - this.getNumEmptySlots()));
                this.tuples[i] = tuple;
                return;
            }
        }

        throw new DbException("This page is full. There have no more empty slots");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    @Override
    public void markDirty(boolean dirty, TransactionId tid) {
        if (dirty) {
            this.dirtyTransaction = tid;
        } else {
            this.dirtyTransaction = null;
        }

    }

    /**
     * Returns the tid of the transaction that last dirtied this page,
     * or null if the page is not dirty
     */
    @Override
    public TransactionId isDirty() {
        return this.dirtyTransaction;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int emptySlots = 0;
        for (byte slots : header) {
            for (int j = 0; j < BYTE_SIZE; j++) {
                if ((slots & (1 << j)) == SLOT_UNUSED) {
                    emptySlots++;
                }
            }
        }
        return emptySlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        final int index = i / BYTE_SIZE;
        final int offset = (i % BYTE_SIZE);
        return (header[index] & (1 << offset)) != SLOT_UNUSED;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        final int index = i / BYTE_SIZE;
        final int offset = (i % BYTE_SIZE);
        if (value) {
            header[index] = (byte) (header[index] | (1 << offset));
        } else {
            header[index] = (byte) (header[index] & ~(1 << offset));
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn'tableId return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        List<Tuple> tupleList = new ArrayList<>();
        for (Tuple tuple : tuples) {
            if (tuple != null) {
                tupleList.add(tuple);
            }
        }
        return tupleList.iterator();
    }
}
