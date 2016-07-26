package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    final int numPages;
    final ConcurrentHashMap<PageId,Page> pages; // hash table storing current pages in memory
    private final Random random = new Random(); // for choosing random pages for eviction

    /** TODO for Lab 4: create your private Lock Manager class. 
	Be sure to instantiate it in the constructor. */
    private final LockManager lockmgr; // Added for Lab 4

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
		// some code goes here
		this.numPages = numPages;
		this.pages = new ConcurrentHashMap<PageId, Page>();
		
		lockmgr = new LockManager(); // Added for Lab 4
    }
    
    public static int getPageSize() {
    	return pageSize;
    }
    
    /**
     * Helper: this should be used for testing only!!!
     */
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) 
    		throws TransactionAbortedException, DbException {
		// some code goes here
		
		
		try {	// Added for Lab 4: acquire the lock on the page first
		    lockmgr.acquireLock(tid, pid, perm);
		} catch (Exception e) { 
			System.out.println(e);
		    throw new TransactionAbortedException(); // caught by callee, who calls transactionComplete()
		}
		
		Page p;
		synchronized(this) {
		    p = pages.get(pid);
		    if(p == null) {
			if(pages.size() >= numPages) {
			    evictPage();// added for lab 2
			    // throw new DbException("Out of buffer pages");
			}
			
			p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
			pages.put(pid, p);
		    }
		}
		return p;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		lockmgr.releaseLock(tid,pid); // Added for Lab 4
    }
    
    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid,true); // Added for Lab 4
    }
    
    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2
		return lockmgr.holdsLock(tid, p); // Added for Lab 4
    }
    
    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
//    	HashSet<PageId> pids = this.lockmgr.getAllPagesByTid(tid);
//    	
//    	for (PageId pid : pids) {
//    		if (commit) {
//    			this.flushPage(pid);
//    		} else {
//    			pages.put(pid, pages.get(pid).getBeforeImage());
//    		}
//    	}
    	
    	try {
	        for (PageId pid : pages.keySet()) {
	            if (pages.get(pid).isDirty() != null && pages.get(pid).isDirty().equals(tid)) {
	                if (commit) {
	                	Page p = pages.get(pid);
	                	
	                    flushPage(pid);
	                } else {
	                	pages.put(pid, pages.get(pid).getBeforeImage());
	                }
	            }
	        }
    	}catch (NullPointerException e){
    		e.printStackTrace();
    		System.exit(0);
    	}
    	
		lockmgr.releaseAllLocks(tid, commit); // Added for Lab 4
    }
    
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		
		DbFile file = Database.getCatalog().getDatabaseFile(tableId);
		
		// let the specific implementation of the file decide which page to add it to
		ArrayList<Page> dirtypages = file.insertTuple(tid, t);
		
		synchronized(this) {
		    for (Page p : dirtypages){
			p.markDirty(true, tid);
			
			// if page in pool already, done.
			if(pages.get(p.getId()) != null) {
			    //replace old page with new one in case insertTuple returns a new copy of the page
			    pages.put(p.getId(), p);
			}
			else {
			    // put page in pool
			    if(pages.size() >= numPages)
				evictPage();
			    pages.put(p.getId(), p);
			}
		    }
		}
    }
    
    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		
		DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
		ArrayList<Page> dirtypages = file.deleteTuple(tid, t);
		
		synchronized(this) {
		    for (Page p : dirtypages){
		    	p.markDirty(true, tid);
		    }
		}
    }
    
    /**
     * Flush all dirty pages to disk.
     * Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for lab1
		
		Iterator<PageId> i = pages.keySet().iterator();
		while(i.hasNext()) {
		    flushPage(i.next());
		}
	
    }
    
    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for labs 1--4
    	this.pages.remove(pid);
    }
    
    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
		// some code goes here
		// not necessary for lab1
		
		Page p = pages.get(pid);
		if (p == null)
		    return; //not in buffer pool -- doesn't need to be flushed
		
		// append an update record to the log, with 
        // a before-image and after-image.
        TransactionId dirtier = p.isDirty();
        if (dirtier != null){
          Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
          Database.getLogFile().force();
        }
		
		DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
		file.writePage(p);
		p.markDirty(false, null);
		
		// use current page contents as the before-image
        // for the next transaction that modifies this page.
        p.setBeforeImage();
    }
    
    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for labs 1--4
    	HashSet<PageId> pages = this.lockmgr.getAllPagesByTid(tid);
    	
    	for (PageId pid : pages) {
    		this.flushPage(pid);
    	}
    }
    
    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
		// some code goes here
		// not necessary for lab1
		
		// try to evict a random page, focusing first on finding one that is not dirty
		// currently does not check for pages with uncommitted xacts, which could impact future labs
		Object pids[] = pages.keySet().toArray();
		PageId pid = (PageId) pids[random.nextInt(pids.length)];
		
		try {
		    Page p = pages.get(pid);
		    if (p.isDirty() != null) { // this one is dirty, try to find first non-dirty
				for (PageId pg : pages.keySet()) {
				    if (pages.get(pg).isDirty() == null) {
						pid = pg;
						break;
				    }
				}
		    }
		    flushPage(pid);
		} catch (IOException e) {
		    throw new DbException("could not evict page");
		}
		pages.remove(pid);
	}
}