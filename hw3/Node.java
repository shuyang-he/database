package hw3;

import hw1.Field;

public interface Node {
	public int getDegree();

	public boolean isLeafNode();

	public boolean isAtCapacity();

	public boolean isOverCapacity();

	public boolean isUnderCapacity();

	public Field getKeyToSelf();
}
