package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private boolean isDeleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
    	this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    }
    /**
     * You can just close and then open the child
     */
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.close();
    	child.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method. You can pass along the TransactionId from the constructor.
     * This operator should keep track of whether its fetchNext() method has been called already. 
     * 
     * @return A 1-field tuple containing the number of deleted records (even if there are 0)
     *          or null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (isDeleted) {
    		return null;
    	}
    	
    	BufferPool buffer = Database.getBufferPool();
    	int cnt = 0;
    	while (child.hasNext()) {
    		Tuple t = child.next();
    		try {
				buffer.deleteTuple(tid, t);
				cnt ++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	isDeleted = true;
    	Tuple tuple = new Tuple(this.getTupleDesc());
    	tuple.setField(0, new IntField(cnt));

    	return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.child = children[0];
    }

}
