package simpledb;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Computes some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field NO_GROUPING_KEY = new IntField(0);

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    // This is only used for AVG aggregation.
    private ConcurrentHashMap<Field, Integer> count;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	if (what != Op.COUNT) {
    		throw new IllegalArgumentException("The input agg type is not COUNT");
    	}
    	
    	this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.count = new ConcurrentHashMap<Field, Integer>();
        
        if (this.gbfield == Aggregator.NO_GROUPING) {
        	this.count.put(NO_GROUPING_KEY, 0);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	if (tup.getTupleDesc().getFieldType(this.gbfield)
    			.equals(this.gbfieldtype)) {
    		Field key = tup.getField(this.gbfield);

    		if (!this.count.containsKey(key)) {
    			this.count.put(key, 0);
    		}

    		this.count.put(key, this.count.get(key) + 1);
    	}
    }

    /**
     * Returns a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	TupleDesc td = new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE });
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	
    	Enumeration<Field> keys = this.count.keys();
    	while (keys.hasMoreElements()) {
    		Tuple t = new Tuple(td);
    		Field key = keys.nextElement();
    		t.setField(0, key);
    		t.setField(1, new IntField(this.count.get(key)));
    		tuples.add(t);
    	}
	
    	return new TupleIterator(td, tuples);
    }

}
