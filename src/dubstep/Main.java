
//package dubstep;

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
import net.sf.jsqlparser.statement.select.SelectItem;

public class Main {

	public static void main(String[] args) throws ParseException, SQLException {

		System.out.print("$>");
		Scanner sc = new Scanner(System.in);
		String tableStructure = sc.nextLine();
		StringReader tableInput = new StringReader(tableStructure);
		CCJSqlParser tableParser = new CCJSqlParser(tableInput);
		Statement tableStatement = tableParser.Statement();
		CreateTable table = (CreateTable) tableStatement;

		String myTableName = table.getTable().getName();
		List<ColumnDefinition> columnNames = table.getColumnDefinitions();

		Map<String, Integer> columnOrderMapping = new HashMap<String, Integer>();
		Map<String, PrimitiveType> columnDataTypeMapping = new HashMap<String, PrimitiveType>();

		int i = 0;
		for (ColumnDefinition col : columnNames) {
			columnOrderMapping.put(col.getColumnName(), i);
			columnDataTypeMapping.put(col.getColumnName(), getPrimitiveValue(col.getColDataType()));
			i++;
		}

		System.out.print("$>");
		while (sc.hasNext()) {
			String inputString = sc.nextLine();
			StringReader input = new StringReader(inputString);
			CCJSqlParser parser = new CCJSqlParser(input);
			try {
				Statement query = parser.Statement();
				if (query instanceof Select) {
					Select select = (Select) query;
					PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
					List<String> selectItems = new ArrayList<String>();
					for(SelectItem sitem: plainSelect.getSelectItems()){
						selectItems.add(sitem.toString());
					}
					
					//System.out.println(selectItems);
					
					if(selectItems.get(0).toString().equals("*")){
						int j = 0;
						for(String s:columnOrderMapping.keySet()){
							if(j==0)
								selectItems.set(0, s);
							else
								selectItems.add(j,s);
							j++;
						}
					}
					List<Integer> selectIndexes = getSelectIndexesfromMap(selectItems, columnOrderMapping);

					Expression e = plainSelect.getWhere();

					readFromFile(myTableName, selectIndexes, columnOrderMapping, columnDataTypeMapping, e);

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

	public static void readFromFile(String tableName, List<Integer> index, Map<String, Integer> columnOrderMapping,
			Map<String, PrimitiveType> columnDataTypeMapping, Expression expr) throws SQLException {
		// File file = new File("data/" + tableName + ".csv");
		File file = new File(tableName + ".csv");
		try {
			Scanner sc = new Scanner(file);
			while (sc.hasNext()) {

				/* read line from csv file */
				String newRow = sc.nextLine();
				/* values array have individual column values from the file */
				String[] values = newRow.split(","); 						// change to | for submission

				/* where clause evaluation */
				Eval eval = new Eval() {
					public PrimitiveValue eval(Column c) {
						/* get this column's index mapping so that we can get
						   the value from the values array 
						 */
						int idx = columnOrderMapping.get(c.toString());
						/* get this column's datatype so that we know what to
						return */
						PrimitiveType ptype = columnDataTypeMapping.get(c.toString());

						return getReturnType(ptype, values[idx]);
					}
				};

				if(!(expr == null)){
					PrimitiveValue ret = eval.eval(expr);
					if ("TRUE".equals(ret.toString())) {
						printToConsole(index, values);
					}	
				}
				else{
					printToConsole(index, values);
				}
				
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void printToConsole(List<Integer> index, String[] values) {

		StringBuilder sbuilder = new StringBuilder();
		for (int i = 0; i < index.size(); i++) {
			sbuilder.append(values[index.get(i)]);
			if (i != index.size() - 1)
				sbuilder.append("|");
		}
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
