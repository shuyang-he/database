package hw2;

import java.util.ArrayList;

import hw1.Field;
import hw1.IntField;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	AggregateOperator op;
	boolean groupBy;
	TupleDesc td;
	ArrayList<Field> groups = new ArrayList<>();
	ArrayList<ArrayList<Field>> groupedLists = new ArrayList<>();
	
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		this.op = o;
		this.groupBy = groupBy;
		this.td = td;
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		Field group;
		Field value;
		if(groupBy) {
			group = t.getField(0);
			value = t.getField(1);
		} else {
			group = new IntField(0);
			value = t.getField(0);
		}
		if(!groups.contains(group)) {
			groups.add(group);
			groupedLists.add(new ArrayList<Field>());
		}
		groupedLists.get(groups.indexOf(group)).add(value);
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		ArrayList<Field> resultList = new ArrayList<>();;
		for(ArrayList<Field> fields: groupedLists) {
			Integer result = null;
			for(Field f: fields) {
				int value = 0;
				if(f.getClass() == IntField.class) {
					value = ((IntField)f).getValue();
				} else if(f.getClass() == StringField.class) {
					String s = ((StringField)f).getValue();
					try {
						value = Integer.parseInt(s);
					}
					catch(NumberFormatException e) {
					}
				} 
				if(result == null) {
					if(op == AggregateOperator.MAX || op == AggregateOperator.MIN) {
						result = value;
					} else {
						result = 0;
					}
				}
				switch(op) {
				case MAX:
					result = value > result ? value : result;
					break;
				case MIN:
					result = value < result ? value : result;
					break;
				case COUNT:
					result++;
					break;
				case SUM:
				case AVG:
					result += value;
				}
			}
			if(op == AggregateOperator.AVG) {
				result /= fields.size();
			}
			Field newField;
			if(fields.get(0).getClass() == IntField.class) {
				newField = new IntField(result);
			} else {
				newField = new StringField(String.valueOf(result));
			}
			resultList.add(newField);
		}
		ArrayList<Tuple> newTuples = new ArrayList<>();
		for(int i = 0; i < groups.size(); i++) {
			Tuple newTuple = new Tuple(td);
			if(groupedLists.get(0).get(0).getClass() == IntField.class) {
				
			}
			if(groupBy) {
				newTuple.setField(0, groups.get(i));
				newTuple.setField(1, resultList.get(i));
			} else {
				newTuple.setField(0, resultList.get(i));
			}
			newTuples.add(newTuple);
		}
		return newTuples;
	}
}
