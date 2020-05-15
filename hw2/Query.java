package hw2;

import java.util.ArrayList;
import java.util.List;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.RelationalOperator;
import hw1.WhereExpressionVisitor;
import hw1.Field;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	private Relation fromItemToRelation(Catalog c, FromItem fromItem) {
		String tableName = ((Table)fromItem).getName();
		HeapFile heapFile = c.getDbFile(c.getTableId(tableName));
		Relation newRelation = new Relation(heapFile.getAllTuples(), heapFile.getTupleDesc());
		return newRelation;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect plainSelect = (PlainSelect)selectStatement.getSelectBody();
		//join relations
		Catalog c = Database.getCatalog();
		Relation resultRelation = fromItemToRelation(c, plainSelect.getFromItem());
		List<Join> joins = plainSelect.getJoins();
		if(joins != null) {
			for(Join join: joins) {
				Relation joinRelation = fromItemToRelation(c, join.getRightItem());
				BinaryExpression onExpression = (BinaryExpression)join.getOnExpression();
				String fieldName1 = ((Column)onExpression.getLeftExpression()).getColumnName();
				String fieldName2 = ((Column)onExpression.getRightExpression()).getColumnName();
				
				int fieldNum1 = resultRelation.getTupleDesc().nameToId(fieldName1);
				int fieldNum2 = joinRelation.getTupleDesc().nameToId(fieldName2);
				resultRelation = resultRelation.join(joinRelation, fieldNum1, fieldNum2);
			}
		}
		//apply where conditions
		Expression whereExpression = plainSelect.getWhere();
		if(whereExpression != null) {
			WhereExpressionVisitor whereExpressionVisitor = new WhereExpressionVisitor();
			whereExpression.accept(whereExpressionVisitor);
			String fieldName = whereExpressionVisitor.getLeft();
			RelationalOperator op = whereExpressionVisitor.getOp();
			Field operand = whereExpressionVisitor.getRight();
			int fieldNum = resultRelation.getTupleDesc().nameToId(fieldName);
			resultRelation = resultRelation.select(fieldNum, op, operand);
		}
		//select columns
		List<SelectItem> selectItems = plainSelect.getSelectItems();
		ArrayList<Integer> columnNums = new ArrayList<>();
		boolean isAggregate = false;
		AggregateOperator op = null;
		ArrayList<Integer> renameNums = new ArrayList<>();
		ArrayList<String> renameNames = new ArrayList<>();
		for(SelectItem si: selectItems) {
			ColumnVisitor columnVisitor = new ColumnVisitor();
			si.accept(columnVisitor);
			String columnName = columnVisitor.getColumn();
			if(columnName == "*") {
				for(int i = 0; i < resultRelation.getDesc().numFields(); i++) {
					columnNums.add(i);
				}
			} else {
				int columnNum = resultRelation.getDesc().nameToId(columnName);
				columnNums.add(columnNum);
				if(columnVisitor.isAggregate()) {
					isAggregate = true;
					op = columnVisitor.getOp();
				}
				Alias alias = ((SelectExpressionItem)si).getAlias();
				if(alias != null && alias.isUseAs()) {
					renameNums.add(columnNum);
					renameNames.add(alias.getName());
				}
			}
		}
		resultRelation = resultRelation.project(columnNums);
		if(isAggregate) {
			boolean groupBy = columnNums.size() == 2;
			resultRelation = resultRelation.aggregate(op, groupBy);
		}
		if(renameNums.size() != 0) {
			resultRelation = resultRelation.rename(renameNums, renameNames);
		}
		return resultRelation;
	}
}
