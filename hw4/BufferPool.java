package hw4;

import java.io.*;
import java.util.*;

import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.Tuple;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
	
	private int numPages;
	private HashMap<Integer, HeapPage> cache;
	private Map<Integer, Set<Integer>> transactionToPages;
	private LockManager lockManager;
	
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // your code here
    	this.numPages = numPages;
    	this.cache = new HashMap<Integer, HeapPage>();
    	this.transactionToPages = new HashMap<Integer, Set<Integer>>();
    	this.lockManager = LockManager.create();
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
     * @param tableId the ID of the table with the requested page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public HeapPage getPage(int tid, int tableId, int pid, Permissions perm)
        throws Exception {
        // your code here
    	this.lockManager.acquireLock(tid, pid, perm);
    	if (cache.containsKey(pid)) {
    		return this.cache.get(pid);
    	} else {
    		if (this.cache.size() >= this.numPages) {
    			evictPage();
    		}
    		HeapPage newPage = Database.getCatalog().getDbFile(tableId).readPage(pid);
			this.cache.put(pid, newPage);
			return newPage;
    	}
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param tableID the ID of the table containing the page to unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(int tid, int tableId, int pid) {
        // your code here
    	this.lockManager.releasePage(tid, pid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(int tid, int tableId, int pid) {
        // your code here
    	return this.lockManager.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction. If the transaction wishes to commit, write
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(int tid, boolean commit)
        throws IOException {
        // your code here
    	if (commit) {
    		Set<Integer> dirtiedFlushedPages = this.transactionToPages.get(tid);
    		for (Map.Entry<Integer, HeapPage> entry : this.cache.entrySet()) {
    			Integer pageId = entry.getKey();
    			HeapPage page = entry.getValue();
    			if (tid == page.isDirty()) {
    	    		flushPage(page.getTableId(), pageId);
    	    		page.setBeforeImage();
    	    	} else if (dirtiedFlushedPages != null && dirtiedFlushedPages.contains(pageId)) {
    	    		page.setBeforeImage();
    	    	}
    		}
    	} else {
    		for (Map.Entry<Integer, HeapPage> entry : this.cache.entrySet()) {
    			Integer pageId = entry.getKey();
    			HeapPage page = entry.getValue();
    		 if (tid == page.isDirty()) {
    		        this.cache.put(pageId, page.getBeforeImage());
    		        page.markDirty(false, -1);
    		    }
    		}
    	}
    	this.transactionToPages.remove(tid);
	    this.lockManager.releasePages(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to. May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(int tid, int tableId, Tuple t)
        throws Exception {
        // your code here
    	for (Map.Entry<Integer, HeapPage> entry : this.cache.entrySet()) {
    		Integer pageId = entry.getKey();
			HeapPage page = entry.getValue();
			int table = page.getTableId();
			boolean hasSpace = page.hasEmptySlot();
			if (table == tableId && hasSpace) {
				page.addTuple(t);
				page.markDirty(true, tid);
				this.cache.put(pageId, page);
				break;
			}
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty.
     *
     * @param tid the transaction adding the tuple.
     * @param tableId the ID of the table that contains the tuple to be deleted
     * @param t the tuple to add
     */
    public void deleteTuple(int tid, int tableId, Tuple t)
        throws Exception {
        // your code here
    	for (Map.Entry<Integer, HeapPage> entry : this.cache.entrySet()) {
    		Integer pageId = entry.getKey();
			HeapPage page = entry.getValue();
			if (page.getTableId() == tableId && page.getId() == t.getId()) {
				page.deleteTuple(t);
				page.markDirty(true, tid);
				break;
			}
			this.cache.put(pageId, page);
    	}
    }

    private synchronized void flushPage(int tableId, int pid) throws IOException {
        // your code here
    	if (this.cache.containsKey(pid)) {
    		HeapPage page = this.cache.get(pid);
    	    Integer dirtier = page.isDirty();
    	    if (dirtier != -1) {
    	    	if (this.transactionToPages.containsKey(dirtier)) {
    	    		this.transactionToPages.get(dirtier).add(pid);
    	    	} else {
    	    	    Set<Integer> pages = new HashSet<Integer>();
    	    	    pages.add(pid);
    	    	    this.transactionToPages.put(dirtier, pages);
    	    	}
    	    	Database.getCatalog().getDbFile(tableId).writePage(page);
    	        page.markDirty(false, -1);
    	    }
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws Exception {
        // your code here
    	HeapPage page = null;
    	Integer pageId = null;
    	for (Map.Entry<Integer, HeapPage> entry : this.cache.entrySet()) {
    		pageId = entry.getKey();
    	    page = entry.getValue();
    	    if (page.isDirty() == -1) {
    	    	pageId = null;
    	    	page = null;
    	    } else {
    	    	break;
    	    }
    	}
    	if (pageId == null || page == null) {
    		throw new Exception("All pages in Buffer Pool are dirty.");
    	}
    	try {
    		flushPage(page.getTableId(), pageId);
    	} catch (IOException e) {
    	    e.printStackTrace();
    	    throw new Exception("Exception while flushing page during eviction.");
    	}
    	this.cache.remove(pageId);
    }

}
