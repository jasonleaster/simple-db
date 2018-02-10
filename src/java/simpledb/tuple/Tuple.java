package simpledb.tuple;

import simpledb.Type;
import simpledb.field.Field;
import simpledb.field.IntField;
import simpledb.field.StringField;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private RecordId recordId;
    private TupleDesc tupleDesc;
    private List<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.tupleDesc = td;
        this.fields = new ArrayList<>();
        initFields(tupleDesc);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\tableId...\tcolumnN
     *
     * where \tableId is any whitespace (except a newline)
     */
    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<TupleDesc.TDItem> tdItems = this.tupleDesc.iterator();
        int i = 0;
        while (tdItems.hasNext()) {
            TupleDesc.TDItem item = tdItems.next();
            stringBuilder.append("FiledName: ").append(item.getFieldName());
            stringBuilder.append("==> Value: ").append(fields.get(i).toString());
            stringBuilder.append("\n");

            i++;
        }
        return stringBuilder.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        initFields(td);
    }

    private void initFields(TupleDesc tupleDesc) {
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            Type fieldType = tupleDesc.getFieldType(i);
            if (fieldType == Type.INT_TYPE) {
                fields.add(new IntField(0));
            } else if (fieldType == Type.STRING_TYPE) {
                fields.add(new StringField("", fieldType.getLen()));
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof  Tuple))
        {
            return false;
        }

        Tuple other = (Tuple) obj;
        if (this.recordId.equals(other.getRecordId()) &&
                this.tupleDesc.equals(other.getTupleDesc())) {
            for (int i = 0; i < this.fields.size(); i++) {
                if (!this.fields.get(i).equals(other.getField(i))) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }
}
