package dubstep;
<<<<<<< HEAD
=======

import java.io.BufferedInputStream;
>>>>>>> db78387f34036945c4aa581dbb77596ab17fa382
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

<<<<<<< HEAD
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

=======
>>>>>>> db78387f34036945c4aa581dbb77596ab17fa382
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
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
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Main {

	public static long avgCount = 0;
	//public static Boolean aggPrint = false;
	public static double aggAns = 0.0;

	public static HashMap<String, SelectItem> selectItemsMap = new HashMap<>();

	public enum AggFunctions { SUM, MIN, MAX, AVG, COUNT }; 
	
	private class TableData {
		Map<String, Integer> columnOrderMapping;
		Map<String, ColDataType> columnDataTypeMapping;

		public Map<String, Integer> getColumnOrderMapping() {
			return columnOrderMapping;
		}

		public void setColumnOrderMapping(Map<String, Integer> columnOrderMapping) {
			this.columnOrderMapping = columnOrderMapping;
		}

		public Map<String, ColDataType> getColumnDataTypeMapping() {
			return columnDataTypeMapping;
		}

		public void setColumnDataTypeMapping(Map<String, ColDataType> columnDataTypeMapping) {
			this.columnDataTypeMapping = columnDataTypeMapping;
		}
	}

	static Main mainObj = new Main();
	static TableData tableData;

	public static Map<String, TableData> tableMapping = new HashMap<String, TableData>();
	public static Map<String, Integer> columnOrderMapping = new HashMap<String, Integer>();
	public static Map<String, ColDataType> columnDataTypeMapping = new HashMap<String, ColDataType>();

	public static List<ColumnDefinition> columnNames = null;
	//public static List<SelectItem> selectItemsAsObject = null;
	public static SelectItem[] selectItemsAsObject = null;
	public static int selCols = 0;

	public static String myTableName = "";
	public static String inputString = "";
	public static String newRow = "";
	public static String ssitem = "";
	public static String aggName = "";
	//public static String[] values = null;

	public static StringReader input = null;

	public static CCJSqlParser parser = null;
	public static Statement query = null;
	public static CreateTable table = null;
	public static Select select = null;
	public static PlainSelect plainSelect = null;
	public static Expression e = null;
	public static Expression selExp = null;
	public static SelectExpressionItem sitem = null;
	public static Function aggregateFunction = null;
	public static Expression aggExpr = null;
	public static PrimitiveValue answer = null;
	public static PrimitiveValue result = null;
	
	public static StringBuilder sbuilder = null;
	
	public static Column aggExprs[] = null;
	public static int numAggFunc = 0;
	public static double aggSum = 0;
	public static long aggCount = 0;
	public static double aggMin = Integer.MAX_VALUE;
	public static double aggMax = Integer.MIN_VALUE;
	public static double aggAvg = 0.0;
	public static double avgTotal = 0.0;
	public static int[] aggNo = null;

	public static AggFunctions aggFunctions;
	//public static Boolean print = null;
	
	public static CSVRecord csvRecord = null;
	
	public static int getAggNo(AggFunctions aggName){
		if(aggName == AggFunctions.SUM){
			return 1;
		}else if(aggName == AggFunctions.MIN){
			return 2;
		}else if(aggName == AggFunctions.MAX){
			return 3;
		}else if(aggName == AggFunctions.AVG){
			return 4;
		}else if(aggName== AggFunctions.COUNT){
			return 5;
		}
		return -1;
	}

	public static void main(String[] args) throws ParseException, SQLException, IOException {

		System.out.print("$>");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		//sc = new Scanner(System.in);

		//inputString = br.readLine();
		while ((inputString = br.readLine()) != null) {

			//inputString = sc.nextLine();
			input = new StringReader(inputString);
			parser = new CCJSqlParser(input);

			try {
				query = parser.Statement();

				if (query instanceof CreateTable) {

					table = (CreateTable) query;

					myTableName = table.getTable().getName();
					tableData = mainObj.new TableData();

					columnNames = table.getColumnDefinitions();

					int i = 0;
					for (ColumnDefinition col : columnNames) {
						columnOrderMapping.put(col.getColumnName(), i);
						columnDataTypeMapping.put(col.getColumnName(), col.getColDataType());
						i++;
					}
					tableData.setColumnDataTypeMapping(columnDataTypeMapping);
					tableData.setColumnOrderMapping(columnOrderMapping);

					tableMapping.put(myTableName, tableData);

				} else if (query instanceof Select) {
					
					//double start = System.currentTimeMillis();

					select = (Select) query;
					plainSelect = (PlainSelect) select.getSelectBody();

					myTableName = plainSelect.getFromItem().toString();
					tableData = tableMapping.get(myTableName);
					columnOrderMapping = tableData.getColumnOrderMapping();
					columnDataTypeMapping = tableData.getColumnDataTypeMapping();

					selectItemsAsObject = new SelectItem[plainSelect.getSelectItems().size()];
					aggNo = new int[plainSelect.getSelectItems().size()];
					aggExprs = new Column[plainSelect.getSelectItems().size()];

					int i = 0, j = 0;
					for (SelectItem sitem : plainSelect.getSelectItems()) {
						
						selExp = ((SelectExpressionItem) sitem).getExpression();
						ssitem = sitem.toString();

						if (selExp instanceof Function) {
							aggName = ((Function) selExp).getName();
							aggFunctions = AggFunctions.valueOf(aggName);
							aggNo[i] = getAggNo(aggFunctions);
							
							if(aggNo[i] != 5){	
								aggExprs[i] = (Column) ((Function) selExp).getParameters().getExpressions().get(0);
							}
							i++;
						}else{
							selectItemsAsObject[j] = sitem;
							j++;
							selectItemsMap.put(sitem.toString(), null);
						}
					}
					selCols = j;
					numAggFunc = i;
										
					readFromFile();
					
<<<<<<< HEAD
					//double end = System.currentTimeMillis();
					//System.out.println("time: " + (end - start)/1000);
=======
//					double end = System.currentTimeMillis();
//					System.out.println("time: " + (end - start)/1000);
					
>>>>>>> db78387f34036945c4aa581dbb77596ab17fa382
				} else {
					// System.out.println("Not of type select");
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
			System.out.print("$>");
		}

	}

	public static void reinitializeValues() {
		avgCount = 0;
		aggAns = 0.0;
		aggCount = 0;
	}

	public static void readFromFile() throws SQLException, IOException {
		//File file = new File("data/" + myTableName + ".csv");
		//File file = new File(myTableName + ".csv");
<<<<<<< HEAD
		CSVParser parser = new CSVParser(new FileReader("data/" + myTableName + ".csv"), CSVFormat.newFormat('|'));
=======

		FileInputStream fis = new FileInputStream(file);
		BufferedInputStream bis = new BufferedInputStream(fis, 32768);
		BufferedReader br = new BufferedReader(new InputStreamReader(bis, StandardCharsets.UTF_8));
>>>>>>> db78387f34036945c4aa581dbb77596ab17fa382
		
		//BufferedReader br = new BufferedReader(new FileReader(file));
		e = plainSelect.getWhere();
		reinitializeValues();
		for (CSVRecord csvR : parser) {
			csvRecord = csvR;
		    
		 
		//Scanner sc = new Scanner(file);
		//while ((newRow = br.readLine()) != null) {

			/* read line from csv file */
			//newRow = sc.nextLine();
			/* values array have individual column values from the file */
			//values = newRow.split("\\|", -1);

			/* where clause evaluation */
			

<<<<<<< HEAD
			if (!(e == null)) {
				PrimitiveValue ret = eval.eval(e);
				if ("TRUE".equals(ret.toString())) {
=======
		Eval eval = new Eval() {
			public PrimitiveValue eval(Column c) {
				/*
				 * get this column's index mapping so that we can get
				 * the value from the values array
				 */
				int idx = columnOrderMapping.get(c.toString());
				/*
				 * get this column's datatype so that we know what to
				 * return
				 */
				String ptype = columnDataTypeMapping.get(c.toString());

				//return getReturnType(ptype, values[idx]);
				return getReturnType(SQLDataType.valueOf(ptype), values[idx]);
			}
		};

		PrimitiveValue ret = null;
		
		try {
			//Scanner sc = new Scanner(file);
			while ((newRow = br.readLine()) != null) {

				/* read line from csv file */
				//newRow = sc.nextLine();
				/* values array have individual column values from the file */
				values = newRow.split("\\|", -1);

				/* where clause evaluation */
				

				if (!(e == null)) {
					ret = eval.eval(e);
					if ("TRUE".equals(ret.toString())) {
						if(numAggFunc > 0){
							printAggToConsole();
						}
						else{	
							printToConsole();
						}
					}
				} else {
>>>>>>> db78387f34036945c4aa581dbb77596ab17fa382
					if(numAggFunc > 0){
						printAggToConsole();
					}
					else{	
						printToConsole();
					}
				}
			} else {
				if(numAggFunc > 0){
					printAggToConsole();
				}
				else{	
					printToConsole();
				}
			}

		} /*
			 * done with file reading...if aggregate function, then print
			 * after this and not in printToConsole
			 */

		if (numAggFunc > 0)
			printAggregateResult();
	}

	/*
	 * sum = 1
	 * min = 2
	 * max = 3
	 * avg = 4
	 * count = 5
	 * */
	private static void printAggregateResult() {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < numAggFunc; i++) {
			
			if (aggNo[i] == 1) {
				sb.append(aggSum);
				sb.append('|');
			} else if (aggNo[i] == 2) {
				sb.append(aggMin);
				sb.append('|');
			} else if (aggNo[i] == 3) {
				sb.append(aggMax);
				sb.append('|');
			} else if (aggNo[i] == 4) {
				sb.append(avgTotal / avgCount);
				sb.append('|');
			} else if (aggNo[i] == 5) {
				sb.append(aggCount);
				sb.append('|');
			}
		}

		if(sb.length() > 0)
			sb.setLength(sb.length() - 1);

		System.out.println(sb);

	}
	
	/*
	 * sum = 1
	 * min = 2
	 * max = 3
	 * avg = 4
	 * count = 5
	 * */

	private static void printAggToConsole() throws SQLException {

		//print = false;
		//aggPrint = true;


		for(int i = 0; i < numAggFunc; i++){
			if (aggNo[i] == 5) {
				aggCount++;

			} else {
				aggExpr = (Expression) aggExprs[i];
				answer = computeExpression();

				if (aggNo[i] == 1) {
					aggSum += answer.toDouble();
				} else if (aggNo[i] == 2 ) {
					if (answer.toDouble() < aggMin) {
						aggMin = answer.toDouble();
					}
				} else if (aggNo[i] == 3 ) {
					if (answer.toDouble() > aggMax) {
						aggMax = answer.toDouble();
					}

				} else if (aggNo[i] == 4 ) {
					avgCount++;
					avgTotal += answer.toDouble();
				}
			}
		
		}
		
	}

	public static Eval eval = new Eval() {
		public PrimitiveValue eval(Column c) {

			int idx = columnOrderMapping.get(c.toString());
			ColDataType ptype = columnDataTypeMapping.get(c.toString());

			return getReturnType(ptype, csvRecord.get(idx));
		}
	};

	
	private static void printToConsole() throws SQLException {

		sbuilder = new StringBuilder();

		for (int i = 0; i < selCols; i++) {
			SelectExpressionItem sitem = (SelectExpressionItem) selectItemsAsObject[i];

			if (selExp instanceof Addition || selExp instanceof Subtraction || selExp instanceof Multiplication
					|| selExp instanceof Division) {

				
				result = eval.eval(selExp);
				sbuilder.append(result);

			} else {
				int idx = columnOrderMapping.get(sitem.toString());
				sbuilder.append(csvRecord.get(idx));
			}

			if (i != selCols - 1)
				sbuilder.append("|");
		}

		
			System.out.println(sbuilder.toString());
		
	}

	static Eval eval = new Eval() {
		public PrimitiveValue eval(Column c) {

			int idx = columnOrderMapping.get(c.toString());
			String ptype = columnDataTypeMapping.get(c.toString());

<<<<<<< HEAD
				int idx = columnOrderMapping.get(c.toString());
				ColDataType ptype = columnDataTypeMapping.get(c.toString());

				return getReturnType(ptype, csvRecord.get(idx));
			}
		};
=======
			//return getReturnType(ptype, values[idx]);
			return getReturnType(SQLDataType.valueOf(ptype), values[idx]);
		}
	};
	static PrimitiveValue expResult = null; 
	
	private static PrimitiveValue computeExpression() throws SQLException {

//		Eval eval = new Eval() {
//			public PrimitiveValue eval(Column c) {
//
//				int idx = columnOrderMapping.get(c.toString());
//				String ptype = columnDataTypeMapping.get(c.toString());
//
//				//return getReturnType(ptype, values[idx]);
//				return getReturnType(SQLDataType.valueOf(ptype), values[idx]);
//			}
//		};
>>>>>>> db78387f34036945c4aa581dbb77596ab17fa382

		expResult = eval.eval(aggExpr);

		return expResult;
	}

	private static PrimitiveValue getReturnType(ColDataType ptype, String value) {

		if (ptype.toString().equalsIgnoreCase("int")) {
			return new LongValue(value);
		} else if (ptype.toString().equalsIgnoreCase("varchar") || ptype.toString().equalsIgnoreCase("char")
				|| ptype.toString().equalsIgnoreCase("string")) {
			return new StringValue(value);
		} else if (ptype.toString().equalsIgnoreCase("date")) {
			return new DateValue(value);
		} else if (ptype.toString().equalsIgnoreCase("decimal")) {
			return new DoubleValue(value);
		}

		return null;
	}

}
