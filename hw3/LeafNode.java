package hw3;

import java.util.ArrayList;

import hw1.Field;
import hw1.RelationalOperator;

public class LeafNode implements Node {
	int degree;
	ArrayList<Entry> entries;

	public LeafNode(int degree) {
		this.degree = degree;
		entries = new ArrayList<Entry>(degree);
	}

	public void setEntries(ArrayList<Entry> entries) {
		this.entries = entries;
	}

	public void deleteEntry(Entry deleteEntry) {
		for (int i = 0; i < entries.size(); i++) {
			if (entries.get(i).getField().compare(RelationalOperator.EQ, deleteEntry.getField())) {
				entries.remove(i);
				return;
			}
		}
	}

	public void addEntry(Entry newEntry) {
		for (int i = 0; i < entries.size(); i++) {
			if (entries.get(i).getField().compare(RelationalOperator.EQ, newEntry.getField())) {
				return;
			}
			if (entries.get(i).getField().compare(RelationalOperator.GT, newEntry.getField())) {
				entries.add(i, newEntry);
				return;
			}
		}
		entries.add(newEntry);
	}

	public boolean containsField(Field searchField) {
		for (Entry entry : entries) {
			if (entry.getField().compare(RelationalOperator.EQ, searchField)) {
				return true;
			}
		}
		return false;
	}

	public boolean isAtCapacity() {
		return entries.size() == (degree + 1) / 2;
	}

	public boolean isUnderCapacity() {
		return entries.size() < (degree + 1) / 2;
	}

	public boolean isOverCapacity() {
		return entries.size() > degree;
	}

	public Field getKeyToSelf() {
		return entries.get(entries.size() - 1).getField();
	}

	public ArrayList<Entry> getEntries() {
		return entries;
	}

	public int getDegree() {
		return degree;
	}

	public boolean isLeafNode() {
		return true;
	}

}