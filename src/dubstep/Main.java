
package dubstep;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

public class Main {
	
	private class TableData{
		Map<String, Integer> columnOrderMapping;
		Map<String, PrimitiveType> columnDataTypeMapping;
		
		public Map<String, Integer> getColumnOrderMapping() {
			return columnOrderMapping;
		}
		public void setColumnOrderMapping(Map<String, Integer> columnOrderMapping) {
			this.columnOrderMapping = columnOrderMapping;
		}
		public Map<String, PrimitiveType> getColumnDataTypeMapping() {
			return columnDataTypeMapping;
		}
		public void setColumnDataTypeMapping(Map<String, PrimitiveType> columnDataTypeMapping) {
			this.columnDataTypeMapping = columnDataTypeMapping;
		}
		
		
	}

	public static void main(String[] args) throws ParseException, SQLException {
		Main mainObj = new Main();
		TableData tableData;
		Map<String, TableData> tableMapping = new HashMap<String, TableData>();
		Map<String, Integer> columnOrderMapping = new HashMap<String, Integer>();
		Map<String, PrimitiveType> columnDataTypeMapping = new HashMap<String, PrimitiveType>();
		String myTableName = "";

		System.out.print("$>");
		Scanner sc = new Scanner(System.in);

		while (sc.hasNext()) {

			String inputString = sc.nextLine();
			StringReader input = new StringReader(inputString);
			CCJSqlParser parser = new CCJSqlParser(input);

			try {
				Statement query = parser.Statement();
				//Statement tableStatement = parser.Statement();
				

				if (query instanceof CreateTable) {
					
					CreateTable table = (CreateTable) query;

					myTableName = table.getTable().getName();
					//System.out.println("tablename: " + myTableName);

					tableData = mainObj.new TableData();
					
					List<ColumnDefinition> columnNames = table.getColumnDefinitions();

					int i = 0;
					for (ColumnDefinition col : columnNames) {
						columnOrderMapping.put(col.getColumnName(), i);
						columnDataTypeMapping.put(col.getColumnName(), getPrimitiveValue(col.getColDataType()));
						i++;
					}
					
					tableData.setColumnDataTypeMapping(columnDataTypeMapping);
					tableData.setColumnOrderMapping(columnOrderMapping);
					
					tableMapping.put(myTableName, tableData);
					
				} else if (query instanceof Select) {
					
					//System.out.println("select for table:  " + myTableName);
					
					Select select = (Select) query;
					
					
					
					PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
					
					myTableName = plainSelect.getFromItem().toString();
					//System.out.println("select cha table name: " + myTableName);
					
					tableData = tableMapping.get(myTableName);
					columnOrderMapping = tableData.getColumnOrderMapping();
					columnDataTypeMapping = tableData.getColumnDataTypeMapping();
					
					List<String> selectItemsAsString = new ArrayList<String>();
					List<SelectItem> selectItemsAsObject = new ArrayList<SelectItem>();

					//System.out.println("this table's mapping: " + columnDataTypeMapping + "\n" + columnOrderMapping);
					
					
					for (SelectItem sitem : plainSelect.getSelectItems()) {
						selectItemsAsString.add(sitem.toString());
						selectItemsAsObject.add(sitem);
					}

					

					if (selectItemsAsString.get(0).toString().equals("*")) {
						int j = 0;
						for (String s : columnOrderMapping.keySet()) {
							if (j == 0)
								selectItemsAsString.set(0, s); // update * with
																// the column
																// name
							else
								selectItemsAsString.add(j, s); // add remaining
																// column names
							j++;
						}
					}
					// List<Integer> selectIndexes =
					// getSelectIndexesfromMap(selectItemsAsString,
					// columnOrderMapping);
					List<Integer> selectIndexes = null;

					Expression e = plainSelect.getWhere();

					readFromFile(myTableName, selectIndexes, selectItemsAsObject, columnOrderMapping,
							columnDataTypeMapping, e);

				} else {
					// System.out.println("Not of type select");
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
			System.out.print("$>");
		}

	}

	private static PrimitiveType getPrimitiveValue(ColDataType datatype) {

		String dtype = datatype.getDataType();
		if (dtype.equals("int")) {
			return PrimitiveType.LONG;
		}
		if (dtype.equals("decimal")) {
			return PrimitiveType.DOUBLE;
		}
		if (dtype.equals("string")) {
			return PrimitiveType.STRING;
		}
		if (dtype.equals("date")) {
			return PrimitiveType.DATE;
		}
		if (dtype.equals("varchar")) {
			return PrimitiveType.STRING;
		}
		if (dtype.equals("char")) {
			return PrimitiveType.STRING;
		}

		return null;
	}

	private static List<Integer> getSelectIndexesfromMap(List<String> items, Map<String, Integer> map) {

		List<Integer> index = new ArrayList<Integer>();

		for (int i = 0; i < items.size(); i++) {
			int num = map.get(items.get(i));
			index.add(num);
		}
		return index;
	}

	public static void readFromFile(String tableName, List<Integer> index, List<SelectItem> selectItemsAsObject,
			Map<String, Integer> columnOrderMapping, Map<String, PrimitiveType> columnDataTypeMapping, Expression expr)
			throws SQLException {
		 File file = new File("data/" + tableName + ".csv");
		//File file = new File(tableName + ".csv");
		try {
			Scanner sc = new Scanner(file);
			while (sc.hasNext()) {

				/* read line from csv file */
				String newRow = sc.nextLine();
				/* values array have individual column values from the file */
				String[] values = newRow.split("|"); // change to | for
														// submission

				/* where clause evaluation */
				Eval eval = new Eval() {
					public PrimitiveValue eval(Column c) {
						/*
						 * get this column's index mapping so that we can get
						 * the value from the values array
						 */
						// System.out.println("column: " + c);
						int idx = columnOrderMapping.get(c.toString());
						/*
						 * get this column's datatype so that we know what to
						 * return
						 */
						PrimitiveType ptype = columnDataTypeMapping.get(c.toString());

						return getReturnType(ptype, values[idx]);
					}
				};

				// System.out.println("expression: " + expr);

				if (!(expr == null)) {
					PrimitiveValue ret = eval.eval(expr);
					if ("TRUE".equals(ret.toString())) {
						printToConsole(index, values, columnOrderMapping, columnDataTypeMapping, selectItemsAsObject);
					}
				} else {
					printToConsole(index, values, columnOrderMapping, columnDataTypeMapping, selectItemsAsObject);
				}

			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void printToConsole(List<Integer> index, String[] values, Map<String, Integer> columnOrderMapping,
			Map<String, PrimitiveType> columnDataTypeMapping, List<SelectItem> selectItemsAsObject)
			throws SQLException {

		// System.out.println("for print\n" + columnOrderMapping + "\t" +
		// selectItemsAsObject );

		StringBuilder sbuilder = new StringBuilder();

		for (int i = 0; i < selectItemsAsObject.size(); i++) {

			SelectExpressionItem sitem = (SelectExpressionItem) selectItemsAsObject.get(i);
			// System.out.println(sitem.getExpression() instanceof Addition);
			Expression selExp = sitem.getExpression();

			if (selExp instanceof Addition || selExp instanceof Subtraction || selExp instanceof Multiplication
					|| selExp instanceof Division) {

				Eval eval = new Eval() {
					public PrimitiveValue eval(Column c) {
						/*
						 * get this column's index mapping so that we can get
						 * the value from the values array
						 */
						// System.out.println("column: " + c);
						int idx = columnOrderMapping.get(c.toString());
						/*
						 * get this column's datatype so that we know what to
						 * return
						 */
						PrimitiveType ptype = columnDataTypeMapping.get(c.toString());

						return getReturnType(ptype, values[idx]);
					}
				};

				PrimitiveValue result = eval.eval(selExp);
				sbuilder.append(result);
				// System.out.println("addition result: " + result);

			} else {
				int idx = columnOrderMapping.get(sitem.toString());
				sbuilder.append(values[idx]);
			}

			if (i != selectItemsAsObject.size() - 1)
				sbuilder.append("|");
		}

		// for (int i = 0; i < index.size(); i++) {
		// sbuilder.append(values[index.get(i)]);
		// if (i != index.size() - 1)
		// sbuilder.append("|");
		// }
		System.out.println(sbuilder.toString());
	}

	private static PrimitiveValue getReturnType(PrimitiveType ptype, String value) {

		if (ptype.equals(PrimitiveType.LONG)) {
			return new LongValue(value);
		}
		if (ptype.equals(PrimitiveType.STRING)) {
			return new StringValue(value);
		}
		if (ptype.equals(PrimitiveType.DATE)) {
			return new DateValue(value);
		}
		if (ptype.equals(PrimitiveType.DOUBLE)) {
			return new DoubleValue(value);
		}

		return null;
	}

}
