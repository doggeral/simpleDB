package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private TupleDesc td;
	private File f;
	private int tableId;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.td = td;
    	this.f = f;
    	this.tableId = f.getAbsoluteFile().hashCode();;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return this.tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	if (this.getId() != pid.getTableId()) {
    		return null;
    	}
    	
    	int pgno = pid.pageNumber();
    	
    	if (pgno < 0 || pgno >= numPages()) {
    		return null;
    	}
    	try {
			RandomAccessFile tableFile = new RandomAccessFile(f, "r");
			int pageSize = BufferPool.getPageSize();
			
			byte data[] = new byte[pageSize];
			tableFile.seek(pgno * pageSize);
			tableFile.readFully(data);
			HeapPageId heapPageId = new HeapPageId(this.tableId, pgno);
			tableFile.close();
			return new HeapPage(heapPageId, data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	try {
			RandomAccessFile f = new RandomAccessFile(this.f, "rw");
			f.seek(page.getId().pageNumber() * BufferPool.getPageSize());
			f.write(page.getPageData(), 0, BufferPool.getPageSize());
			f.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	return (int) Math.ceil((double)f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	// Get a page which has empty slot
    	HeapPage page = null;
    	boolean getPage = false;
    	ArrayList<Page> pageList = new ArrayList<Page>();
    	
    	
    	int pageno = this.numPages();
    	BufferPool buffer = Database.getBufferPool();
    	for (int i = 0; i < pageno; i++) {
    		page = (HeapPage) buffer.getPage(tid, new HeapPageId(this.tableId, i), Permissions.READ_WRITE);
    		
    		if (page.getNumEmptySlots() > 0) {
    			getPage = true;
    		}
    	}
    	
    	if (getPage) {
    		page.insertTuple(t);
    		pageList.add(page);
    	} else {
    		HeapPage newpage = new HeapPage(new HeapPageId(this.tableId, pageno), HeapPage.createEmptyPageData());
    		newpage.insertTuple(t);
    		this.writePage(newpage);
    		pageList.add(newpage);
    	}
    	
    	return pageList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<Page> pageList = new ArrayList<Page>();
    	BufferPool buffer = Database.getBufferPool();
    	HeapPage page = (HeapPage) buffer.getPage(tid, 
    			new HeapPageId(this.tableId, t.getRecordId().getPageId().pageNumber()), Permissions.READ_WRITE);
    	
    	page.deleteTuple(t);
    	pageList.add(page);
    	
    	return pageList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
}

