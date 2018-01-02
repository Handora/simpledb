package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> schema = null;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return schema.iterator();
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
        schema = new ArrayList<TDItem>();

        for (int i = 0; i < typeAr.length; i++) {
            schema.add(new TDItem(typeAr[i], fieldAr[i]));
        }
        // some code goes here
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
        // some code goes here
        schema = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            schema.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return schema.size();
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
        if (i >= numFields() || i < 0) {
            throw new NoSuchElementException("No such element");
        }

        return schema.get(i).fieldName;
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
        if (i >= numFields() || i < 0) {
            throw new NoSuchElementException("No such element");
        }

        return schema.get(i).fieldType;
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
        // some code goes here
        if (schema == null || name == null)
	        throw new NoSuchElementException("No such element");
	    for (int i=0; i<numFields(); i++) {
	        if (name.equals(schema.get(i).fieldName)) {
    		    return i;
	        }
	    }

	    throw new NoSuchElementException("No such element");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
	    int sum = 0;
	    for (TDItem t: schema) {
	        sum += t.fieldType.getLen();
	    }
	    return sum;
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
        // some code goes here
        Type[] ts = new Type[td1.numFields()+td2.numFields()];
        String[] ns = new String[td1.numFields()+td2.numFields()];
        for (int i=0; i<td1.numFields(); i++) {
            ns[i] = td1.getFieldName(i);
            ts[i] = td1.getFieldType(i);
        }
        for (int i=0; i<td2.numFields(); i++) {
            ns[td1.numFields()+i] = td2.getFieldName(i);
            ts[td1.numFields()+i] = td2.getFieldType(i);
        }
        TupleDesc td = new TupleDesc(ts, ns);
        return td;
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

    public boolean equals(Object o) {
        // some code goes here
        if (o == null)
            return false;

        if (!(o instanceof TupleDesc))
            return false;

        TupleDesc td = (TupleDesc)o;

        if (numFields() == td.numFields()) {
            for (int i=0; i<numFields(); i++) {
                if (schema.get(i).fieldType != td.getFieldType(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // TODO:
        //      may have better hashCode
        return Objects.hashCode(schema);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String ret = "";

        for (int i=0; i<numFields(); i++) {
            if (i == 0)
            ret += schema.get(i).fieldType+"("+schema.get(i).fieldName+")";
            else
            ret += ", "+schema.get(i).fieldType+"("+schema.get(i).fieldName+")";
        }
        return ret;
    }

    /**
     * personal addition:
     */
    public void setFieldName(int i, String s) {
      if (i<0 || i >= numFields()) {
        return;
      }
      TDItem t = schema.get(i);
      TDItem newt = new TDItem(t.fieldType, s);
      schema.set(i, newt);
    }
}
