package simpledb.tuple;

import simpledb.Type;
import simpledb.field.Field;
import simpledb.page.Page;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

   private List<TDItem> tdItems = new ArrayList<>();

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        private final Type fieldType;
        
        /**
         * The name of the field
         * */
        private final String fieldName;

        TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        public Type getFieldType() {
            return fieldType;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        for (int i = 0; i < typeAr.length; i++) {
            if (fieldAr == null) {
                tdItems.add(new TDItem(typeAr[i], null));
            } else {
                tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.numFields()) {
            throw new NoSuchElementException("Index Out Of Range. " +
                    "The length of this tuple is: " + tdItems.size());
        }
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.numFields()) {
            throw new NoSuchElementException("Index Out Of Range. " +
                    "The length of this tuple is: " + tdItems.size());
        }
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        /*
            考虑name中包含表名 TableName.fieldName的情况
         */
        if (name != null && !name.isEmpty() && name.contains(".")) {
            String[] vals = name.split("[.]");
            name = vals[vals.length - 1];
        }

        for (int i = 0; i < tdItems.size(); i++) {
            TDItem tdItem = tdItems.get(i);
            if (tdItem.fieldName != null && tdItem.fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("There is no corresponding element which name is :" + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (TDItem item : tdItems) {
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        if (td1 == null) {
            return td2;
        }

        if (td2 == null) {
            return td1;
        }

        int newTupleLen = td1.numFields() + td2.numFields();
        Type[] types = new Type[newTupleLen];
        String[] names = new String[newTupleLen];

        for (int i = 0; i < td1.numFields(); i++) {
            types[i] = td1.getFieldType(i);
            names[i] = td1.getFieldName(i);
        }
        int fieldsOfTD1 = td1.numFields();
        for (int i = 0; i < td2.numFields(); i++) {
            types[fieldsOfTD1 + i] = td2.getFieldType(i);
            names[fieldsOfTD1 + i] = td2.getFieldName(i);
        }

        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof  TupleDesc)) {
            return false;
        }

        TupleDesc other = (TupleDesc) o;
        if (other.getSize() != this.getSize() || other.numFields() != this.numFields()) {
            return false;
        }

        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getFieldType(i).equals(other.getFieldType(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (TDItem item : tdItems) {
            result += item.toString().hashCode() *  37 ;
        }
        return result;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < this.numFields(); i++) {
            TDItem tdItem =  tdItems.get(i);
            stringBuilder.append("(")
                    .append(tdItem.fieldType.toString())
                    .append("[").append(i).append("]")
                    .append("(").append(tdItem.fieldName)
                    .append("[").append(i).append("])");
        }
        return stringBuilder.toString();
    }
}
