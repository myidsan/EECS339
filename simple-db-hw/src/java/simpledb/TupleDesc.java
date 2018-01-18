package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private ArrayList<TDItem> fields;
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

    /**
     * @return
     *        An iterator which iterates over all the field fields
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        // return Array.asList(this.fields).iterator();
        return fields.iterator();
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
        // some code goes here
        fields = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
          TDItem field = new TDItem(typeAr[i], fieldAr[i]);
          fields.add(field);
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
        // some code goes here
        fields = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
          TDItem field = new TDItem(typeAr[i], "");
          fields.add(field);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fields.size();
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
        if (i < 0 || i > fields.size() - 1) {
          throw new NoSuchElementException("index out of range");
        }
        return fields.get(i).fieldName;
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
        if (i < 0 || i > fields.size() - 1) {
          throw new NoSuchElementException("indiex out of range");
        }
        return fields.get(i).fieldType;
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
        for (TDItem field: fields) {
          int index = fields.indexOf(field);
          if ( field.fieldName != null && field.fieldName.equals(name) ) {
            return index;
          } else {
            if ( name == null ) {
              throw new NoSuchElementException("null");
            }
          }
        }
        throw new NoSuchElementException("No field with matching name");
      }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem field : fields) {
          size += field.fieldType.getLen();
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
        // some code goes here
        Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
        String[] fieldAr = new String[td1.numFields() + td2.numFields()];
        int i, j;
        for ( i = 0; i < td1.numFields(); i++ ) {
          typeAr[i] = td1.getFieldType(i);
          fieldAr[i] = td1.getFieldName(i);
        }
        for ( j = 0; j < td2.numFields(); j++ ) {
          typeAr[i] = td2.getFieldType(j);
          fieldAr[i] = td2.getFieldName(j);
          i++;
        }
        return new TupleDesc(typeAr, fieldAr);
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
        if ( o instanceof TupleDesc ) {
          TupleDesc obj = (TupleDesc)o;
          //test size
          if (( obj.getSize() == this.getSize() )) {
            for (int i=0; i < obj.numFields(); i++ ) {
              if ( !this.getFieldType(i).equals(obj.getFieldType(i)) ) {
                // different type
                return false;
              }
            }
            // same type
            return true;
          } else {
            // size mismatch
            return false;
          }
        } else {
          // o is not TupleDesc
          return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
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
        String str = "";
        for (int i = 0; i < fields.size(); i++) {
          str += fields.get(i) + ", ";
        }
        return str;
    }
}
