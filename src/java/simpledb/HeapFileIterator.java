package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private TransactionId tid;
    private HeapFile hf;

    private boolean active;
    private int currentPageNo;
    private Iterator<Tuple> currentPageIter;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.tid = tid;
        this.hf = hf;
        close();
    }

    private int numPages() {
        return hf.numPages();
    }

    public void open() throws DbException, TransactionAbortedException {
        active = true;
        currentPageNo = -1;
        currentPageIter = null;
        System.out.println("!!!" + Database.getBufferPool());
        while (currentPageNo + 1 < numPages()) {
            currentPageNo ++;
            System.out.println("!!!" + Database.getBufferPool().getPage(
                    tid, new HeapPageId(hf.getId(), currentPageNo), Permissions.READ_ONLY));
            currentPageIter = ((HeapPage)Database.getBufferPool().getPage(
                    tid, new HeapPageId(hf.getId(), currentPageNo), Permissions.READ_ONLY)).iterator();
            if (!hasNext()) continue;
            return;
        }
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        return (currentPageIter != null) && (currentPageIter.hasNext());
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!active) throw new NoSuchElementException("Iterator has not been opened.");
        Tuple ans = (hasNext()) ? currentPageIter.next() : null;
        if (!hasNext()) {
            while (currentPageNo + 1 < numPages()) {
                currentPageNo ++;
                currentPageIter = ((HeapPage)Database.getBufferPool().getPage(
                        tid, new HeapPageId(hf.getId(), currentPageNo), Permissions.READ_ONLY)).iterator();
                if (!hasNext()) continue;
                break;
            }
        }
        return ans;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        if (!active) throw new DbException("Iterator has not been opened.");
        close();
        open();
    }

    public TupleDesc getTupleDesc() {
        return hf.getTupleDesc();
    }

    public void close() {
        active = false;
        currentPageNo = -1;
        currentPageIter = null;
    }
}