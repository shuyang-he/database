package hw2;

import java.util.ArrayList;
import java.util.Arrays;

import hw1.Field;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;
import hw1.RelationalOperator;;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		this.tuples = l;
		this.td = td;
	}
	
	public TupleDesc getTupleDesc() {
		return td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		ArrayList<Tuple> newTuples = new ArrayList<>();
		for(Tuple t: tuples) {
			if(t.getField(field).compare(op, operand)) {
				newTuples.add(t);
			}
		}
		return new Relation(newTuples, td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		Type[] tdTypes = td.getTypes();
		String[] tdFields = td.getFields();
		for(int i = 0; i < fields.size(); i++) {
			int fieldNum = fields.get(i);
			tdFields[fieldNum] = names.get(i);
		}
		TupleDesc newTd = new TupleDesc(tdTypes, tdFields);
		ArrayList<Tuple> newTuples = tuples;
		for(Tuple t: newTuples) {
			t.setDesc(newTd);
		}
		return new Relation(newTuples, newTd);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		int size = fields.size();
		Type[] newTypes = new Type[size];
		String[] newFields = new String[size];
		for(int i = 0; i < size; i++) {
			int fieldNum = fields.get(i);
			newTypes[i] = td.getType(fieldNum);
			newFields[i] = td.getFieldName(fieldNum);
		}
		TupleDesc newTd = new TupleDesc(newTypes, newFields);
		ArrayList<Tuple> newTuples = new ArrayList<>();
		for(Tuple t: tuples) {
			Tuple newT = new Tuple(newTd);
			for(int i = 0; i < size; i++) {
				int fieldNum = fields.get(i);
				newT.setField(i, t.getField(fieldNum));
			}
			newTuples.add(newT);
		}
		return new Relation(newTuples, newTd);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int fieldNum1, int fieldNum2) {
		String[] fields1 = td.getFields();
		String[] fields2 = other.td.getFields();
		Type[] types1 = td.getTypes();
		Type[] types2 = other.td.getTypes();
		
		String[] fields = Arrays.copyOf(fields1, fields1.length + fields2.length);
		System.arraycopy(fields2, 0, fields, fields1.length, fields2.length);
		Type[] types = Arrays.copyOf(types1, types1.length + types2.length);
		System.arraycopy(types2, 0, types, types1.length, types2.length);
		
		TupleDesc newTd = new TupleDesc(types, fields);
		ArrayList<Tuple> newTuples = new ArrayList<>();
		for(Tuple t1: tuples) {
			for(Tuple t2: other.tuples) {
				if(t1.getField(fieldNum1).compare(RelationalOperator.EQ, t2.getField(fieldNum2))) {
					Tuple newTuple = new Tuple(newTd);
					for(int i = 0; i < fields1.length; i++) {
						newTuple.setField(i, t1.getField(i));
					}
					for(int i = 0; i < fields2.length; i++) {
						newTuple.setField(i, t2.getField(i));
					}
					newTuples.add(newTuple);
				}
			}
		}
		return new Relation(newTuples, newTd);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		Aggregator aggregator = new Aggregator(op, groupBy, td);
		for(Tuple t: tuples) {
			aggregator.merge(t);
		}
		return new Relation(aggregator.getResults(), td);
	}
	
	public TupleDesc getDesc() {
		return td;
	}
	
	public ArrayList<Tuple> getTuples() {
		return tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		String s = td.toString() + '\n';
		for(Tuple t: tuples) {
			s += t.toString() + '\n';
		}
		return s;
	}
}
