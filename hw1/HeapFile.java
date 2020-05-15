package hw1;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A heap file stores a collection of tuples. It is also responsible for
 * managing pages. It needs to be able to manage page creation as well as
 * correctly manipulating pages when tuples are added or deleted.
 * 
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {

	public static final int PAGE_SIZE = 4096;

	private File filePath;
	private TupleDesc td;
	public List<HeapPage> pages;
	private int id;

	/**
	 * Creates a new heap file in the given location that can accept tuples of
	 * the given type
	 * 
	 * @param f
	 *            location of the heap file
	 * @param types
	 *            type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		filePath = f;
		td = type;
		pages = new ArrayList<HeapPage>();
		try {
			RandomAccessFile file = new RandomAccessFile(filePath, "rw");
			for (int pid = 0; pid * PAGE_SIZE < file.length(); pid++) {
				pages.add(readPage(pid));
			}
			file.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			RandomAccessFile file = new RandomAccessFile(filePath, "rw");
			id = file.hashCode();
			file.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
//		System.out.println("id at file creation: "+getId());
	}

	public File getFile() {
		return filePath;
	}

	public TupleDesc getTupleDesc() {
		return td;
	}

	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a
	 * RandomAccessFile object should be used here.
	 * 
	 * @param id
	 *            the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		try {
			RandomAccessFile file = new RandomAccessFile(filePath, "rw");
			file.seek(id * PAGE_SIZE);
			byte[] bytes = new byte[PAGE_SIZE];
			file.read(bytes);
			file.close();
			return new HeapPage(id, bytes, getId(), td);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Returns a unique id number for this heap file. Consider using the hash of
	 * the File itself.
	 * 
	 * @return
	 */
	public int getId() {
		return id;
	}

	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through
	 * the file, a RandomAccessFile object should be used in this method.
	 * 
	 * @param p
	 *            the page to write to disk
	 * @throws IOException
	 */
	public void writePage(HeapPage p) throws IOException {
		RandomAccessFile file = new RandomAccessFile(filePath, "rw");
		file.seek(p.getId() * PAGE_SIZE);
		// System.out.println("***" + p.getId());
		file.write(p.getPageData());
		file.close();
	}

	/**
	 * Adds a tuple. This method must first find a page with an open slot,
	 * creating a new page if all others are full. It then passes the tuple to
	 * this page to be stored. It then writes the page to disk (see writePage)
	 * 
	 * @param t
	 *            The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		for (HeapPage hp : pages) {
			if (hp.hasEmptySlot()) {
				try {
					hp.addTuple(t);
					//writePage(hp);
					return hp;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		try {
			HeapPage newPage = new HeapPage(pages.size(), t, getId());
			pages.add(newPage);
			//writePage(newPage);
			return newPage;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * This method will examine the tuple to find out where it is stored, then
	 * delete it from the proper HeapPage. It then writes the modified page to
	 * disk.
	 * 
	 * @param t
	 *            the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t) {
		int pid = t.getPid();
		try {
			pages.get(pid).deleteTuple(t);
			//writePage(pages.get(pid));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It
	 * must access each HeapPage to do this (see iterator() in HeapPage)
	 * 
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		ArrayList<Tuple> allTuples = new ArrayList<Tuple>();
		for (HeapPage hp : pages) {
			Iterator<Tuple> iter = hp.iterator();
			while (iter.hasNext()) {
				allTuples.add(iter.next());
			}
		}
		return allTuples;
	}

	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * 
	 * @return the number of pages
	 */
	public int getNumPages() {
		return pages.size();
	}
}
