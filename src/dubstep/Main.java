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
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Main {
	
	public static long avgCount = 0;
	public static Boolean aggPrint = false;
	public static double aggAns = 0.0;
	
	//public static HashMap<String, Double> aggAnswersMap= new HashMap<String, Double>();
	public static HashMap<String, SelectItem> selectItemsMap = new HashMap<>();
	
	
	private class TableData{
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
	public static List<SelectItem> selectItemsAsObject = null;				

	public static String myTableName = "";
	public static String inputString = "";
	public static String newRow = "";
	public static String ssitem = "";
	public static String aggName = "";
	public static String[] values = null;
	
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
	
	public static Scanner sc = null;
	public static StringBuilder sbuilder = null;
	
	public static double aggSum = 0;
  public static long aggCount = 0;
  public static double aggMin = Integer.MAX_VALUE;
  public static double aggMax = Integer.MIN_VALUE;
  public static double aggAvg = 0.0;
  public static double avgTotal = 0.0;
    
	
	public static Boolean print = null;
	
	public static void main(String[] args) throws ParseException, SQLException {

		System.out.print("$>");
		sc = new Scanner(System.in);

		while (sc.hasNext()) {

			inputString = sc.nextLine();
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
					System.out.println();
					tableData.setColumnDataTypeMapping(columnDataTypeMapping);
					tableData.setColumnOrderMapping(columnOrderMapping);
					
					tableMapping.put(myTableName, tableData);
					
				} else if (query instanceof Select) {
										
					select = (Select) query;	
					plainSelect = (PlainSelect) select.getSelectBody();
					
					myTableName = plainSelect.getFromItem().toString();					
					tableData = tableMapping.get(myTableName);
					columnOrderMapping = tableData.getColumnOrderMapping();
					columnDataTypeMapping = tableData.getColumnDataTypeMapping();
					
					selectItemsAsObject = new ArrayList<SelectItem>();					
					
					for (SelectItem sitem : plainSelect.getSelectItems()) {
						selectItemsAsObject.add(sitem);
						selectItemsMap.put(sitem.toString(), null);
					}
					
					e = plainSelect.getWhere();
					//aggAnswersMap = new HashMap<String, Double>();
					
					readFromFile();
					
				} else {
					// System.out.println("Not of type select");
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
			System.out.print("$>");
		}

	}

//	private static PrimitiveType getPrimitiveValue(ColDataType datatype) {
//
//		String dtype = datatype.getDataType();
//		if (dtype.equals("int")) {
//			return PrimitiveType.LONG;
//		}
//		if (dtype.equals("decimal")) {
//			return PrimitiveType.DOUBLE;
//		}
//		if (dtype.equals("string")) {
//			return PrimitiveType.STRING;
//		}
//		if (dtype.equals("date")) {
//			return PrimitiveType.DATE;
//		}
//		if (dtype.equals("varchar")) {
//			return PrimitiveType.STRING;
//		}
//		if (dtype.equals("char")) {
//			return PrimitiveType.STRING;
//		}
//
//		return null;
//	}

	public static void reinitializeValues(){
		avgCount = 0;		
		aggAns = 0.0;
	}
	
	public static void readFromFile() throws SQLException {
		 File file = new File("data/" + myTableName + ".csv");
		//File file = new File(myTableName + ".csv");
		
		reinitializeValues();
		
		try {
			Scanner sc = new Scanner(file);
			while (sc.hasNext()) {

				/* read line from csv file */
				newRow = sc.nextLine();
				/* values array have individual column values from the file */
				values = newRow.split("\\|", -1); 
				
				/* where clause evaluation */
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
						ColDataType ptype = columnDataTypeMapping.get(c.toString());

						return getReturnType(ptype, values[idx]);
					}
				};

				if (!(e == null)) {
					PrimitiveValue ret = eval.eval(e);
					if ("TRUE".equals(ret.toString())) {
						printToConsole();
					}
				} else {
					printToConsole();
				}
				
			}/*done with file reading...if aggregate function, then print after this and not in printToConsole*/
			
			if(aggPrint)
				printAggregateResult(selectItemsAsObject);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
//		finally{
//			sc.close();
//		}
	}

	private static void printAggregateResult(List<SelectItem> selectItemsAsObject) {
			
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < selectItemsAsObject.size(); i++) {

			sitem = (SelectExpressionItem) selectItemsAsObject.get(i);
			selExp = sitem.getExpression();
			ssitem = sitem.toString();
			
			aggName = ((Function) selExp).getName();
			if("SUM".equalsIgnoreCase(aggName)){
				sb.append(aggSum);
				sb.append('|');
			}else if("MIN".equalsIgnoreCase(aggName)){
				sb.append(aggMin);
				sb.append('|');
			}else if("MAX".equalsIgnoreCase(aggName)){
				sb.append(aggMax);
				sb.append('|');
			}else if("AVG".equalsIgnoreCase(aggName)){
				sb.append(avgTotal/avgCount);
				sb.append('|');
			}else if("COUNT".equalsIgnoreCase(aggName)){
				sb.append(aggCount);
				sb.append('|');
			}
		}
		
		sb.setLength(sb.length() - 1);
		
		System.out.println(sb);
		
	}

	private static void printToConsole() throws SQLException {

		sbuilder = new StringBuilder();

		print = true;
		for (int i = 0; i < selectItemsAsObject.size(); i++) {

			SelectExpressionItem sitem = (SelectExpressionItem) selectItemsAsObject.get(i);
			Expression selExp = sitem.getExpression();

			String ssitem = sitem.toString();
//			if(!aggAnswersMap.containsKey(ssitem)){
//				if(sitem.toString().contains("MIN") || sitem.toString().contains("min")){
//					aggAnswersMap.put(ssitem, (double) Integer.MAX_VALUE);
//				}else{
//					aggAnswersMap.put(ssitem, 0.0);		
//				}
//			}
			print = true;
			aggPrint = false;
			if(selExp instanceof Function){
				
				print = false;
				aggPrint = true;
				
				aggregateFunction = (Function) selExp;
				aggName = aggregateFunction.getName();
				
				if ("COUNT".equalsIgnoreCase(aggName)) {
//					aggAns = aggAnswersMap.get(ssitem);
//					aggAns++;
//					aggAnswersMap.put(ssitem, aggAns);
					
					aggCount++;
					
				} else {

					aggExpr = aggregateFunction.getParameters().getExpressions().get(0);
					answer = computeExpression();
					
					//aggAns = aggAnswersMap.get(ssitem);
					
					if ("SUM".equalsIgnoreCase(aggName)) {
						//aggAns += answer.toDouble();
						aggSum += answer.toDouble();
						
					} else if ("MIN".equalsIgnoreCase(aggName)) {
//						if(answer.toDouble() < aggAns){
//							aggAns = answer.toDouble();
//						}
						
						if(answer.toDouble() < aggMin){
							aggMin = answer.toDouble();
						}
						
					} else if ("MAX".equalsIgnoreCase(aggName)) {
//						if(answer.toDouble() > aggAns){
//							aggAns = answer.toDouble();
//						}
						
						if(answer.toDouble() > aggMax){
							aggMax = answer.toDouble();
						}
						
					} else if("AVG".equalsIgnoreCase(aggName)){
						avgCount++;
						//aggAns = aggAns/avgCount;
						avgTotal += answer.toDouble();
						
					}
					
					//aggAnswersMap.put(ssitem, aggAns);

				}
			}

			else if (selExp instanceof Addition || selExp instanceof Subtraction || selExp instanceof Multiplication
					|| selExp instanceof Division) {

				Eval eval = new Eval() {
					public PrimitiveValue eval(Column c) {
						
						int idx = columnOrderMapping.get(c.toString());
						ColDataType ptype = columnDataTypeMapping.get(c.toString());

						return getReturnType(ptype, values[idx]);
					}
				};

				PrimitiveValue result = eval.eval(selExp);
				sbuilder.append(result);

			} else {
				int idx = columnOrderMapping.get(sitem.toString());
				sbuilder.append(values[idx]);
			}

			if (i != selectItemsAsObject.size() - 1)
				sbuilder.append("|");
		}

		if(print){
			System.out.println(sbuilder.toString());
		}
	}
	
	private static PrimitiveValue computeExpression() throws SQLException{
		
		Eval eval = new Eval() {
			public PrimitiveValue eval(Column c) {
				
				int idx = columnOrderMapping.get(c.toString());
				ColDataType ptype = columnDataTypeMapping.get(c.toString());

				return getReturnType(ptype, values[idx]);
			}
		};

		PrimitiveValue result = eval.eval(aggExpr);
		
		return result;
	}

	private static PrimitiveValue getReturnType(ColDataType ptype, String value) {

		if (ptype.toString().equalsIgnoreCase("int")) {
			return new LongValue(value);
		}else if (ptype.toString().equalsIgnoreCase("varchar") || ptype.toString().equalsIgnoreCase("char") || ptype.toString().equalsIgnoreCase("string") ) {
			return new StringValue(value);
		}else if (ptype.toString().equalsIgnoreCase("date")) {
			return new DateValue(value);
		}else if (ptype.toString().equalsIgnoreCase("decimal")) {
			return new DoubleValue(value);
		}

		return null;
	}

}
