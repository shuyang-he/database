package hw1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HeapPage {

	private int id;
	private byte[] header;
	private Tuple[] tuples;
	private TupleDesc td;
	private int numSlots;
	private int tableId;
	
	private byte[] oldData;
	private Byte oldDataLock = new Byte((byte) 0);
	private boolean isDirty;
	private int transactionId;

	public HeapPage(int id, byte[] data, int tableId, TupleDesc td) throws IOException {
		this.id = id;
		this.tableId = tableId;

		this.td = td;
		this.numSlots = getNumSlots();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i = 0; i < header.length; i++)
			header[i] = dis.readByte();

		try {
			// allocate and read the actual records of this page
			tuples = new Tuple[numSlots];
			for (int i = 0; i < tuples.length; i++)
				tuples[i] = readNextTuple(dis, i);
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		}
		dis.close();
		
		this.isDirty = false;
	    this.transactionId = -1;
	    setBeforeImage();
	}

	public HeapPage(int id, Tuple t, int tableId) throws Exception {
		this.id = id;
		this.tableId = tableId;
		this.td = t.getDesc();
		this.numSlots = getNumSlots();
		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		tuples = new Tuple[numSlots];
		addTuple(t);
		
		this.isDirty = false;
	    this.transactionId = -1;
	    setBeforeImage();
	}

	public int getId() {
		return id;
	}
	
	public int getTableId() {
		return this.tableId;
	}

	/**
	 * Computes and returns the total number of slots that are on this page
	 * (occupied or not). Must take the header into account!
	 * 
	 * @return number of slots on this page
	 */
	public int getNumSlots() {
		return (int) (HeapFile.PAGE_SIZE / (td.getSize() + (float) 1 / 8));
	}

	/**
	 * Computes the size of the header. Headers must be a whole number of bytes
	 * (no partial bytes)
	 * 
	 * @return size of header in bytes
	 */
	private int getHeaderSize() {
		return (getNumSlots() + 7) / 8;
	}

	/**
	 * Checks to see if a slot is occupied or not by checking the header
	 * 
	 * @param s
	 *            the slot to test
	 * @return true if occupied
	 */
	public boolean slotOccupied(int s) {
		return ((header[s / 8] >> (s % 8)) & 1) == 1;
	}

	/**
	 * Sets the occupied status of a slot by modifying the header
	 * 
	 * @param s
	 *            the slot to modify
	 * @param value
	 *            its occupied status
	 */
	public void setSlotOccupied(int s, boolean value) {
		if (value) {
			header[s / 8] |= (1 << (s % 8));
		} else {
			header[s / 8] &= ~(1 << (s % 8));
		}
	}

	public boolean hasEmptySlot() {
		for (int i = 0; i < getNumSlots(); i++) {
			if (!slotOccupied(i)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Adds the given tuple in the next available slot. Throws an exception if
	 * no empty slots are available. Also throws an exception if the given tuple
	 * does not have the same structure as the tuples within the page.
	 * 
	 * @param t
	 *            the tuple to be added.
	 * @throws Exception
	 */
	public void addTuple(Tuple t) throws Exception {
		if (t.getDesc().getSize() != td.getSize()) {
			throw new Exception("Tuple insertion size mismatch!");
		}
		for (int y = 0; y < this.header.length; y++) {
			for (int i = 0; i < 8; i++) {
				if (!slotOccupied(y * 8 + i)) {
					this.tuples[y * 8 + i] = t;
					setSlotOccupied(y * 8 + i, true);
					return;
				}
			}
		}
		throw new Exception("No empty slots available!");
	}

	/**
	 * Removes the given Tuple from the page. If the page id from the tuple does
	 * not match this page, throw an exception. If the tuple slot is already
	 * empty, throw an exception
	 * 
	 * @param t
	 *            the tuple to be deleted
	 * @throws Exception
	 */
	public void deleteTuple(Tuple t) throws Exception {
		if (t.getPid() != id) {
			throw new Exception("Tuple deletion pid mismatch!");
		}
		int slot = t.getId();
		if (!slotOccupied(slot)) {
			throw new Exception("Tuple multiple deletion!");
		}
		setSlotOccupied(slot, false);
		this.tuples[slot] = null;
	}

	/**
	 * Suck up tuples from the source file.
	 */
	private Tuple readNextTuple(DataInputStream dis, int slotId) {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!slotOccupied(slotId)) {
			for (int i = 0; i < td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(td);
		t.setPid(this.id);
		t.setId(slotId);

		for (int j = 0; j < td.numFields(); j++) {
			if (td.getType(j) == Type.INT) {
				byte[] field = new byte[4];
				try {
					dis.read(field);
					t.setField(j, new IntField(field));
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				byte[] field = new byte[129];
				try {
					dis.read(field);
					t.setField(j, new StringField(field));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return t;
	}

	/**
	 * Generates a byte array representing the contents of this page. Used to
	 * serialize this page to disk.
	 *
	 * The invariant here is that it should be possible to pass the byte array
	 * generated by getPageData to the HeapPage constructor and have it produce
	 * an identical HeapPage object.
	 *
	 * @return A byte array correspond to the bytes of this page.
	 */
	public byte[] getPageData() {
		int len = HeapFile.PAGE_SIZE;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the header of the page
		for (int i = 0; i < header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i = 0; i < tuples.length; i++) {

			// empty slot
			if (!slotOccupied(i)) {
				for (int j = 0; j < td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j = 0; j < td.numFields(); j++) {
				System.out.println(i + ":" + tuples.length);
				Field f = tuples[i].getField(j);
				try {
					dos.write(f.toByteArray());

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = HeapFile.PAGE_SIZE - (header.length + td.getSize() * tuples.length); // -
																							// numSlots
																							// *
																							// td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Returns an iterator that can be used to access all tuples on this page.
	 * 
	 * @return
	 */
	public Iterator<Tuple> iterator() {
		List<Tuple> accessibleTuples = new ArrayList<Tuple>();
		for (int i = 0; i < tuples.length; i++) {
			if (slotOccupied(i)) {
				accessibleTuples.add(tuples[i]);
			}
		}
		return accessibleTuples.iterator();
	}
	
	// record the origin data.
	public void setBeforeImage() {
	    synchronized (this.oldDataLock) {
	    	this.oldData = getPageData().clone();
	    }
	}
	
	// get the origin data.
	public HeapPage getBeforeImage() {
	    try {
	    	byte[] oldDataRef = null;
	    	synchronized (this.oldDataLock) {
	    		oldDataRef = this.oldData;
	    	}
	    	return new HeapPage(this.id, oldDataRef, this.tableId, this.td);
	    } catch (IOException e) {
	    	e.printStackTrace();
	    	System.exit(1);
	    }
	    return null;
	}
	
	// mark the page dirty state.
	public void markDirty(boolean dirty, int tid) {
	    this.isDirty = dirty;
	    this.transactionId = isDirty ? tid : -1;
	}
	
	// return the transaction id for buffer pool.
	public int isDirty() {
	    return isDirty ? this.transactionId : -1;
	}
}
