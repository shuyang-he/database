package hw3;

import java.util.ArrayList;
import java.util.Stack;

import hw1.Field;

public class BPlusTree {
	int degree;
	Node root;

	public BPlusTree(int degree) {
		this.degree = degree;
		root = new LeafNode(degree);
	}

	public LeafNode search(Field f) {
		Node node = root;
		while (!node.isLeafNode()) {
			node = ((InnerNode) node).selectChild(f);
		}
		LeafNode resultNode = (LeafNode) node;
		if (!resultNode.containsField(f)) {
			resultNode = null;
		}
		return resultNode;
	}

	private void resolveOverCapacity(InnerNode parentNode, Node childNode) {
		if (childNode.isOverCapacity()) {
			Node newNode;
			if (childNode.isLeafNode()) {
				LeafNode leafChildNode = (LeafNode) childNode;
				ArrayList<Entry> allEntries = leafChildNode.getEntries();
				int mid = (allEntries.size() + 1) / 2;
				ArrayList<Entry> newEntries1 = new ArrayList<Entry>(allEntries.subList(0, mid));
				ArrayList<Entry> newEntries2 = new ArrayList<Entry>(allEntries.subList(mid, allEntries.size()));
				leafChildNode.setEntries(newEntries1);
				LeafNode newLeafNode = new LeafNode(degree);
				newLeafNode.setEntries(newEntries2);
				newNode = newLeafNode;
			} else {
				InnerNode innerChildNode = (InnerNode) childNode;
				ArrayList<Node> allChildren = innerChildNode.getChildren();
				int mid = (allChildren.size() + 1) / 2;
				ArrayList<Node> newChildren1 = new ArrayList<Node>(allChildren.subList(0, mid));
				ArrayList<Node> newChildren2 = new ArrayList<Node>(allChildren.subList(mid, allChildren.size()));
				innerChildNode.setChildren(newChildren1);
				InnerNode newInnerNode = new InnerNode(degree);
				newInnerNode.setChildren(newChildren2);
				newNode = newInnerNode;
			}
			if (parentNode != null) {
				parentNode.insertNode(newNode);
			} else {
				InnerNode newRoot = new InnerNode(degree);
				newRoot.insertNode(childNode);
				newRoot.insertNode(newNode);
				root = newRoot;
			}
		}
	}

	private void insertEntryInto(Entry e, Node node) {
		if (node.isLeafNode()) {
			LeafNode leafNode = (LeafNode) node;
			leafNode.addEntry(e);
		} else {
			InnerNode innerNode = (InnerNode) node;
			Node nextNode = innerNode.selectChild(e.getField());
			insertEntryInto(e, nextNode);
			resolveOverCapacity(innerNode, nextNode);
			innerNode.updateKeys();
		}
	}

	public void insert(Entry e) {
		insertEntryInto(e, root);
		resolveOverCapacity(null, root);
	}

	private Node findSiblingFromLeftOrRight(Stack<Node> nodeStack, boolean left) {
		@SuppressWarnings("unchecked")
		Stack<Node> localStack = (Stack<Node>) nodeStack.clone();
		Node childNode;
		InnerNode parentNode;
		Node siblingParent = null;
		int level = 0;
		while (localStack.size() >= 2) {
			childNode = localStack.pop();
			parentNode = (InnerNode) localStack.peek();
			Node sibling;
			if (left) {
				sibling = parentNode.getLeftSiblingOfChild(childNode);
			} else {
				sibling = parentNode.getRightSiblingOfChild(childNode);
			}
			if (sibling != null) {
				siblingParent = sibling;
				break;
			}
			level++;
		}
		if (siblingParent == null) {
			return null;
		}
		if (left) {
			for (; level > 0; level--) {
				siblingParent = ((InnerNode) siblingParent).getRightmostChild();
			}
		} else {
			for (; level > 1; level--) {
				siblingParent = ((InnerNode) siblingParent).getLeftmostChild();
			}
			if (level > 0) {
				siblingParent = ((InnerNode) siblingParent).getChildren().get(1);
			}
		}
		return siblingParent;
	}

	private Node findSibling(Stack<Node> nodeStack) {
		Node sibling = findSiblingFromLeftOrRight(nodeStack, true);
		if (sibling == null) {
			sibling = findSiblingFromLeftOrRight(nodeStack, false);
		}
		return sibling;
	}

	private void resolveUnderCapacity(Stack<Node> nodeStack) {
		if (nodeStack.peek() == root) {
			if (root.isLeafNode()) {
				return;
			}
			InnerNode rootInnerNode = (InnerNode) root;
			if (rootInnerNode.getChildren().size() < 2) {
				root = rootInnerNode.getChildren().get(0);
			}
			return;
		}
		if (!nodeStack.peek().isUnderCapacity()) {
			return;
		}
		Node childNode = nodeStack.pop();
		InnerNode parentNode = (InnerNode) nodeStack.peek();
		nodeStack.push(childNode);
		Node siblingNode = findSibling(nodeStack);
		if (siblingNode.isLeafNode()) {
			LeafNode childLeafNode = (LeafNode) childNode;
			LeafNode siblingLeafNode = (LeafNode) siblingNode;
			if (siblingNode.isAtCapacity()) {
				parentNode.removeChild(childNode);
				for (Entry e : childLeafNode.getEntries()) {
					siblingLeafNode.addEntry(e);
				}
			} else {
				ArrayList<Entry> siblingEntries = siblingLeafNode.getEntries();
				Entry borrowedEntry = siblingEntries.get(siblingEntries.size() - 1);
				childLeafNode.addEntry(borrowedEntry);
				siblingLeafNode.deleteEntry(borrowedEntry);
			}
		} else { // InnerNode
			InnerNode childInnerNode = (InnerNode) childNode;
			InnerNode siblingInnerNode = (InnerNode) siblingNode;
			if (siblingNode.isAtCapacity()) {
				parentNode.removeChild(childNode);
				for (Node movedNode : childInnerNode.getChildren()) {
					siblingInnerNode.insertNode(movedNode);
				}
			} else {
				ArrayList<Node> siblingChildren = siblingInnerNode.getChildren();
				Node borrowedChild = siblingChildren.get(siblingChildren.size() - 1);
				childInnerNode.insertNode(borrowedChild);
				siblingInnerNode.removeChild(borrowedChild);
			}
		}
	}

	private void deleteEntryFrom(Entry e, Stack<Node> nodeStack) {
		Node node = nodeStack.peek();
		if (node.isLeafNode()) {
			LeafNode leafNode = (LeafNode) node;
			leafNode.deleteEntry(e);
		} else {
			InnerNode innerNode = (InnerNode) node;
			Node nextNode = innerNode.selectChild(e.getField());
			nodeStack.add(nextNode);
			deleteEntryFrom(e, nodeStack);
		}
		resolveUnderCapacity(nodeStack);
		nodeStack.pop();
	}

	private void updateAllKeys(Node parentNode) {
		if (parentNode.isLeafNode()) {
			return;
		}
		InnerNode parentInnerNode = (InnerNode) parentNode;
		for (Node childNode : parentInnerNode.getChildren()) {
			updateAllKeys(childNode);
		}
		parentInnerNode.updateKeys();
	}

	public void delete(Entry e) {
		Stack<Node> nodeStack = new Stack<Node>();
		nodeStack.add(root);
		deleteEntryFrom(e, nodeStack);
		updateAllKeys(root);
	}

	public Node getRoot() {
		return root;
	}

}
