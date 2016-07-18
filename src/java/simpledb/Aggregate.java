package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private DbIterator aggIter;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of fetchNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	// some code goes here
    	this.child = child;
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    	this.aggIter = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	// some code goes here
    	
    	return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	// some code goes here
    	if (groupField() == -1) {
    		return null;
    	} else {
    		return child.getTupleDesc().getFieldName(gfield);
    	}
    	
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	// some code goes here
    	return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	// some code goes here
    	return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	// some code goes here
    	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	// some code goes here
    	super.open();
    	child.open();
    	
    	Aggregator aggregator = null;
    	Type gType = gfield == Aggregator.NO_GROUPING ? null : this.child.getTupleDesc().getFieldType(gfield);
    			
    	if (this.child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
    		aggregator = new IntegerAggregator(this.gfield, gType, this.afield, this.aop);
		} else {
			aggregator = new StringAggregator(this.gfield, gType, this.afield, this.aop);
		}
		
		while (child.hasNext()) {
			aggregator.mergeTupleIntoGroup(child.next());
		}

		aggIter = aggregator.iterator();
		aggIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     *
     * Hint: think about how many tuples you have to process from the child operator
     * before this method can return its first tuple.
     * Hint: notice that you each Aggregator class has an iterator() method
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	// some code goes here
    	if (aggIter.hasNext()) {
    		return aggIter.next();
    	} else {
    		return null;
    	}
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	// some code goes here
    	this.aggIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	// some code goes here
    	if (this.gfield == Aggregator.NO_GROUPING) {
    		return new TupleDesc(new Type[] { Type.INT_TYPE });
    	} else {
    		return new TupleDesc(new Type[] { child.getTupleDesc().getFieldType(gfield), Type.INT_TYPE });
    	}
    }

    public void close() {
    	// some code goes here
    	super.close();
    	child.close();
    	this.aggIter = null;
    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public DbIterator[] getChildren() {
    	// some code goes here
    	return new DbIterator[]{child};
    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public void setChildren(DbIterator[] children) {
    	// some code goes here
    	this.child = children[0];
    }
    
}
