package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

import static java.nio.file.StandardCopyOption.*;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.IntField;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

public class HeapFileTest {

	private HeapFile hf;
	private TupleDesc td;
	private Catalog c;
	
	@Before
	public void setup() {
		System.out.println("setup running!!!!!!!!");
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(),
					StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}

		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		int tableId = c.getTableId("test");
		System.out.println("getTableId: "+tableId);
		System.out.println("&&&"+c.getDbFile(tableId).getAllTuples().size());
		td = c.getTupleDesc(tableId);
		hf = c.getDbFile(tableId);
		//System.out.println("&&&&&&"+hf.getAllTuples().size());
	}

	@Test
	public void testGetters() {
		assertTrue(hf.getTupleDesc().equals(td));

		assertTrue(hf.getNumPages() == 1);
		assertTrue(hf.readPage(0) != null);
	}

	@Test
	public void testWrite() {
		Tuple t = new Tuple(td);
		t.setField(0, new IntField(new byte[] { 0, 0, 0, (byte) 131 }));
		byte[] s = new byte[129];
		s[0] = 2;
		s[1] = 98;
		s[2] = 121;
		t.setField(1, new StringField(s));
		System.out.print("**+**");
		System.out.println(t);
		try {
			hf.addTuple(t);
		} catch (Exception e) {
			e.printStackTrace();
			fail("unable to add valid tuple");
		}

		assertTrue(hf.getAllTuples().size() == 2);
	}

	@Test
	public void testRemove() {
		Tuple t = new Tuple(td);
		t.setField(0, new IntField(new byte[] { 0, 0, (byte) 2, (byte) 18 }));
		byte[] s = new byte[129];
		s[0] = 2;
		s[1] = 0x68;
		s[2] = 0x69;
		t.setField(1, new StringField(s));
		System.out.println(hf.getAllTuples().size());
		for (Tuple tuple : hf.getAllTuples()) {
			System.out.println(tuple);
		}
		try {
			System.out.print("**-**");
			System.out.println(t);
			hf.deleteTuple(t);
			System.out.println(hf.getAllTuples().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("unable to delete tuple");
		}
		for (Tuple tuple : hf.getAllTuples()) {
			System.out.println(tuple);
		}
		System.out.println(hf.pages.size());
		assertTrue(hf.getAllTuples().size() == 0);
	}

}
