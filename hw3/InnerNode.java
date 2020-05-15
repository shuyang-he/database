package hw3;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import hw1.Field;
import hw1.RelationalOperator;

public class InnerNode implements Node {
	int degree;
	ArrayList<Field> keys;
	ArrayList<Node> children;

	public InnerNode(int degree) {
		this.degree = degree;
		keys = new ArrayList<Field>(degree);
		children = new ArrayList<Node>(degree + 1);
	}

	public void updateKeys() {
		ArrayList<Field> newKeys = new ArrayList<Field>();
		for (int i = 0; i < children.size() - 1; i++) {
			newKeys.add(children.get(i).getKeyToSelf());
		}
		keys = newKeys;
	}

	public void setChildren(ArrayList<Node> newNodes) {
		children = newNodes;
		updateKeys();
	}

	public void insertNode(Node newNode) {
		Field newKey = newNode.getKeyToSelf();
		for (int i = 0; i < children.size(); i++) {
			if (children.get(i).getKeyToSelf().compare(RelationalOperator.GTE, newKey)) {
				children.add(i, newNode);
				updateKeys();
				return;
			}
		}
		children.add(newNode);
		updateKeys();
		return;
	}

	public void removeChild(Node expiringNode) {
		children.remove(children.indexOf(expiringNode));
		// TODO: update keys?
	}

	public Node selectChild(Field newKey) {
		for (int i = 0; i < keys.size(); i++) {
			if (keys.get(i).compare(RelationalOperator.GTE, newKey)) {
				return children.get(i);
			}
		}
		return children.get(keys.size());
	}

	public boolean isAtCapacity() {
		return children.size() == (degree + 1) / 2;
	}

	public boolean isUnderCapacity() {
		return children.size() < (degree + 1) / 2;
	}

	public boolean isOverCapacity() {
		return children.size() > degree + 1;
	}

	public Node getLeftmostChild() {
		return children.get(0);
	}

	public Node getRightmostChild() {
		return children.get(children.size() - 1);
	}

	public Node getRightSiblingOfChild(Node child) {
		int index = children.indexOf(child);
		if (index < 0) {
			throw new NoSuchElementException();
		} else if (index + 1 >= children.size()) {
			return null;
		} else {
			return children.get(index + 1);
		}
	}

	public Node getLeftSiblingOfChild(Node child) {
		int index = children.indexOf(child);
		if (index < 0) {
			throw new NoSuchElementException();
		} else if (index == 0) {
			return null;
		} else {
			return children.get(index - 1);
		}
	}

	public Field getKeyToSelf() {
		return children.get(children.size() - 1).getKeyToSelf();
	}

	public ArrayList<Field> getKeys() {
		return keys;
	}

	public ArrayList<Node> getChildren() {
		return children;
	}

	public int getDegree() {
		return degree;
	}

	public boolean isLeafNode() {
		return false;
	}

}