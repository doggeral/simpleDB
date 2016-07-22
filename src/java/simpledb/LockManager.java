package simpledb;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages locks on PageIds held by TransactionIds. S-locks and X-locks are
 * represented as Permissions.READ_ONLY and Permisions.READ_WRITE, respectively
 *
 * All the field read/write operations are protected by this
 * 
 * @Threadsafe
 */
public class LockManager {

	final int LOCK_WAIT = 10; // ms
	private ConcurrentHashMap<PageId, TransactionId> exclusiveLockMap;
	private ConcurrentHashMap<PageId, HashSet<TransactionId>> shareLockMap;
	private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> waitList;
	private ConcurrentHashMap<TransactionId, HashSet<PageId>> transactionPageMap;

	/**
	 * Sets up the lock manager to keep track of page-level locks for
	 * transactions Should initialize state required for the lock table data
	 * structure(s)
	 */
	public LockManager() {
		// some code here
		exclusiveLockMap = new ConcurrentHashMap<PageId, TransactionId>();
		shareLockMap = new ConcurrentHashMap<PageId, HashSet<TransactionId>>();
		waitList = new ConcurrentHashMap<TransactionId, HashSet<TransactionId>>();
		transactionPageMap = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();
	}

	/**
	 * Tries to acquire a lock on page pid for transaction tid, with permissions
	 * perm. If cannot acquire the lock, waits for a timeout period, then tries
	 * again.
	 *
	 * In Exercise 5, checking for deadlock will be added in this method Note
	 * that a transaction should throw a DeadlockException in this method to
	 * signal that it should be aborted.
	 *
	 * @throws DeadlockException
	 *             after on cycle-based deadlock
	 */
	@SuppressWarnings("unchecked")
	public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
			throws Exception {

		while (!lock(tid, pid, perm)) { // keep trying to get the lock
			synchronized (this) {
				// some code here for Exercise 5, deadlock detection

			}

			try {
				Thread.sleep(LOCK_WAIT); // couldn't get lock, wait for some
											// time, then try again
			} catch (InterruptedException e) {
			}

		}

		synchronized (this) {
			// for Exercise 5, might need some cleanup on deadlock detection
			// data structure
		}

		return true;
	}

	/**
	 * Release all locks corresponding to TransactionId tid. Check lab
	 * description to make sure you clean up appropriately depending on whether
	 * transaction commits or aborts
	 */
	public synchronized void releaseAllLocks(TransactionId tid, boolean commit) {
		// some code here

	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public synchronized boolean holdsLock(TransactionId tid, PageId p) {
		// some code here
		if (this.transactionPageMap.containsKey(tid)) {
			if (this.transactionPageMap.get(tid).contains(p)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Answers the question: is this transaction "locked out" of acquiring lock
	 * on this page with this perm? Returns false if this tid/pid/perm lock
	 * combo can be achieved (i.e., not locked out), true otherwise.
	 * 
	 * Logic:
	 *
	 * if perm == READ if tid is holding any sort of lock on pid, then the tid
	 * can acquire the lock (return false).
	 *
	 * if another tid is holding a READ lock on pid, then the tid can acquire
	 * the lock (return false). if another tid is holding a WRITE lock on pid,
	 * then tid can not currently acquire the lock (return true).
	 *
	 * else if tid is THE ONLY ONE holding a READ lock on pid, then tid can
	 * acquire the lock (return false). if tid is holding a WRITE lock on pid,
	 * then the tid already has the lock (return false).
	 *
	 * if another tid is holding any sort of lock on pid, then the tid can not
	 * currenty acquire the lock (return true).
	 */
	private synchronized boolean locked(TransactionId tid, PageId pid,
			Permissions perm) {
		// some code here
		if (perm == Permissions.READ_ONLY) {

			if (transactionPageMap.containsKey(tid)) {
				if (transactionPageMap.get(tid).contains(pid)) {
					return false;
				}
			}

			if (shareLockMap.containsKey(pid)) {
				return false;
			}

			if (exclusiveLockMap.containsKey(pid)) {
				return true;
			}

		} else {
			HashSet<TransactionId> transactionList = shareLockMap.get(pid);
			if (transactionList != null && transactionList.size() == 1
					&& transactionList.contains(tid)) {
				return false;
			}

			if (exclusiveLockMap.containsKey(pid)
					&& !exclusiveLockMap.get(pid).equals(tid)
					|| shareLockMap.containsKey(pid)) {
				return true;
			}

		}

		return false;
	}

	/**
	 * Releases whatever lock this transaction has on this page Should update
	 * lock table
	 *
	 * Note that you do not need to "wake up" another transaction that is
	 * waiting for a lock on this page, since that transaction will be
	 * "sleeping" and will wake up and check if the page is available on its own
	 * However, if you decide to change the fact that a thread is sleeping in
	 * acquireLock(), you would have to wake it up here
	 */
	public synchronized void releaseLock(TransactionId tid, PageId pid) {
		// some code here
		if (shareLockMap.containsKey(pid)) {
			shareLockMap.get(pid).remove(tid);
		}
		exclusiveLockMap.remove(pid);
		if (transactionPageMap.containsKey(tid)) {
			transactionPageMap.get(tid).remove(pid);
		}
	}

	/**
	 * Attempt to lock the given PageId with the given Permissions for this
	 * TransactionId Should update the lock table
	 *
	 * Returns true if the attempt was successful, false otherwise
	 */
	private synchronized boolean lock(TransactionId tid, PageId pid,
			Permissions perm) {

		if (locked(tid, pid, perm)) {
			return false; // this transaction cannot get the lock on this page;
							// it is "locked out"
		}

		// some code here
		if (perm == Permissions.READ_ONLY) {
			if (this.shareLockMap.containsKey(pid)) {
				shareLockMap.get(pid).add(tid);
			} else {
				shareLockMap.put(pid, new HashSet<TransactionId>() {
					{
						add(tid);
					}
				});
			}

		} else {
			this.exclusiveLockMap.put(pid, tid);
			// Since pid has the exclusive lock, so remove the share lock.
			this.shareLockMap.remove(pid);
		}

		if (transactionPageMap.containsKey(tid)) {
			transactionPageMap.get(tid).add(pid);
		} else {
			transactionPageMap.put(tid, new HashSet<PageId>() {
				{
					add(pid);
				}
			});
		}

		return true;
	}
}