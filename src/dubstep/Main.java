
package dubstep;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.TreeMap;

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
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Main {

	public static long avgCount = 0;
	public static Boolean aggPrint = false;
	public static double aggAns = 0.0;

	public static HashMap<String, SelectItem> selectItemsMap = new HashMap<>();

	public enum AggFunctions {
		SUM, MIN, MAX, AVG, COUNT, sum, min, max, avg, count
	};

	public enum SQLDataType {
		string, varchar, sqlchar, sqlint, DECIMAL, DATE, decimal, date
	};

	public static Main mainObj = new Main();
	public static TableData tableData = null;

	public static Map<String, TableData> tableMapping = new HashMap<String, TableData>();
	public static Map<String, Integer> columnOrderMapping = new HashMap<String, Integer>();
	public static Map<String, String> columnDataTypeMapping = new HashMap<String, String>();
	public static Map<String, Map> columnIndex = new HashMap<String, Map>();
	public static Map<String, Double[]> aggGroupByMap = new HashMap<String, Double[]>();

	public static List<Column> groupByElementsList = new ArrayList<Column>();

	public static boolean orderOperator = false;
	public static List<SubSelect> innerSelects = new ArrayList<>();

	public static List<ColumnDefinition> columnNames = null;
	// public static List<SelectItem> selectItemsAsObject = null;
	public static SelectItem[] selectItemsAsObject = null;
	public static int selCols = 0;

	public static String myTableName = "";
	public static String inputString = "";
	public static String newRow = "";
	public static String ssitem = "";
	public static String aggName = "";
	public static String[] values = null;
	public static boolean selectStar = false;
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

	// public static Column aggExprs[] = null;
	public static Expression aggExprs[] = null;
	public static int numAggFunc = 0;
	public static double aggSum = 0;
	public static long aggCount = 0;
	public static double aggMin = Integer.MAX_VALUE;
	public static double aggMax = Integer.MIN_VALUE;
	public static double aggAvg = 0.0;
	public static double avgTotal = 0.0;
	public static int[] aggNo = null;

	public static AggFunctions aggFunctions;
	public static SQLDataType sqlDataType;
	public static Boolean print = null;
	public static Boolean outermost = false;

	public static ProcessQueries pq = null;
	public static MyCreateTable ct = null;

	public static long limit = 0;
	public static long count = 0;

	public static Map<String, String> primaryKeyIndex = new HashMap<String, String>();
	public static List<String> primaryKeyList = new ArrayList<>();

	public static List<String> orderByElementsList = new ArrayList<String>();
	public static Map<String, Integer> orderByElementsSortOrder = new HashMap<>();
	
	public static boolean isDone = false;

	public static int getAggNo(AggFunctions aggName) {
		if (aggName == AggFunctions.SUM || aggName == AggFunctions.sum) {
			return 1;
		} else if (aggName == AggFunctions.MIN || aggName == AggFunctions.min) {
			return 2;
		} else if (aggName == AggFunctions.MAX || aggName == AggFunctions.max) {
			return 3;
		} else if (aggName == AggFunctions.AVG || aggName == AggFunctions.avg) {
			return 4;
		} else if (aggName == AggFunctions.COUNT || aggName == AggFunctions.count) {
			return 5;
		}
		return -1;
	}

	private static PrimitiveValue getReturnType(SQLDataType ptype, String value) {

		if (ptype == SQLDataType.sqlint) {
			return new LongValue(value);
		} else if (ptype == SQLDataType.varchar || ptype == SQLDataType.sqlchar || ptype == SQLDataType.string) {
			return new StringValue(value);
		} else if (ptype == SQLDataType.DATE || ptype == SQLDataType.date) {
			return new DateValue(value);
		} else if (ptype == SQLDataType.DECIMAL || ptype == SQLDataType.decimal) {
			return new DoubleValue(value);
		}

		return null;
	}

	public static void main(String[] args) throws ParseException, SQLException, IOException {

		System.out.print("$>");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		/*
		 * keep reading from the grader
		 */
		while ((inputString = br.readLine()) != null) {

			input = new StringReader(inputString);
			parser = new CCJSqlParser(input);

			try {
				query = parser.Statement();

				// create table query
				if (query instanceof CreateTable) {

					ct = new MyCreateTable();
					ct.createTable();

				} else if (query instanceof Select) { // select queries

					reinitializeValues();
					innerSelects = new ArrayList<>(); // stores nested select
														// statements
					pq = new ProcessQueries();

					selectStar = false;

					select = (Select) query;
					plainSelect = (PlainSelect) select.getSelectBody();

					// orderByElementsList = plainSelect.getOrderByElements();
					groupByElementsList = plainSelect.getGroupByColumnReferences();

					// System.out.println("gb::" + groupByElementsList);

					limit = -1;
					count = 0;

					if (plainSelect.getOrderByElements() != null) {
						orderByElementsList = new ArrayList<String>();

						for (OrderByElement o : plainSelect.getOrderByElements()) {
							if (o.isAsc()) {
								orderByElementsList.add(o.toString());
								orderByElementsSortOrder.put(o.toString(), 1);

							} else {
								String orderByElement[] = o.toString().split(" ");
								orderByElementsList.add(orderByElement[0]);
								orderByElementsSortOrder.put(orderByElement[0], 2);
							}
						}

						orderOperator = true; /*
												 * tells us from where to read
												 * the data::file or map
												 */
					}

					if (plainSelect.getLimit() != null) {
						limit = plainSelect.getLimit().getRowCount();
					}
					// System.out.println("limit: " + limit);

					/*
					 * check if there are inner select statements
					 */
					FromItem fromItem = plainSelect.getFromItem();
					if (fromItem instanceof Table) {
						// no inner select
						outermost = true;
						pq.processInnermostSelect(myTableName);
					} else {
						String alias = fromItem.getAlias();

						if (!alias.equals("") || alias != null) {

							HashMap<String, String> tempMap = new HashMap<>();
							// tableName.columnName -->> alias.columnName

							for (Entry<String, String> c : columnDataTypeMapping.entrySet()) {
								String key = c.getKey();
								String value = c.getValue();
								key = key.replace(myTableName, alias);
								tempMap.put(key, value);
							}
							
							HashMap<String, Integer> tempMap2 = new HashMap<>();

							for (Entry<String, Integer> c : columnOrderMapping.entrySet()) {
								String key = c.getKey();
								Integer value = c.getValue();
								key = key.replace(myTableName, alias);
								tempMap2.put(key, value);
							}

							List<String> tempList = new ArrayList<>();
							for (String pk : primaryKeyList) {
								String newpk = pk.replace(myTableName, alias);
								tempList.add(newpk);
							}

							TableData td = new TableData();

							td.setColumnDataTypeMapping(tempMap);
							td.setColumnOrderMapping(tempMap2);
							td.setPrimaryKeyList(tempList);

							tableMapping.put(alias, td);

							pq.populateInnerSelectStatements((SubSelect) plainSelect.getFromItem());
							pq.processInnermostSelect(alias);
						} else {
							pq.populateInnerSelectStatements((SubSelect) plainSelect.getFromItem());
							pq.processInnermostSelect(myTableName);
						}

					}

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
		aggSum = 0;
		aggMax = Integer.MIN_VALUE;
		aggMin = Integer.MAX_VALUE;
		avgTotal = 0.0;

		aggGroupByMap = new HashMap<>();

		orderOperator = false;
	}

	public static void readFromFile() throws SQLException, IOException {

		/*
		 * read from the file directly, as no order by clause is present
		 */
		if (orderOperator == false) {

			File file;
			if (System.getProperty("user.home").contains("deepti")) {
				System.out.println("local");
				file = new File(myTableName + ".csv");
			} else {

				file = new File("data/" + myTableName + ".csv");
			}

			BufferedReader br = new BufferedReader(new FileReader(file));
			// get the where clause
			e = plainSelect.getWhere();

			// reinitializeValues();

			PrimitiveValue ret = null;

			try {

				while ((newRow = br.readLine()) != null) {

					processReadFromFile(ret);
				}

				/*
				 * done with file reading...if aggregate function, then print
				 * after this and not in printToConsole
				 */

				if (numAggFunc > 0)
					printAggregateResult();

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

		} else {// order by present, read from the maps created
			e = plainSelect.getWhere();
			reinitializeValues();
			PrimitiveValue ret = null;
			TreeMap orderIndexMap = new TreeMap<>();

			String firstOrderOperator = orderByElementsList.get(0);
			if (columnIndex.containsKey(firstOrderOperator)) {
				orderIndexMap = (TreeMap) columnIndex.get(firstOrderOperator);
			} else {
				// if index not built on order by column, build it on the fly
				MyCreateTable.sortMyTable(firstOrderOperator, tableData.getPrimaryKeyList());
				orderIndexMap = (TreeMap) columnIndex.get(firstOrderOperator);
			}

			// iterate through sorted hashmap to fetch rows

			Iterator iterator = null;
			if (orderByElementsSortOrder.get(firstOrderOperator) == 1) {
				iterator = orderIndexMap.entrySet().iterator();
			} else {
				iterator = orderIndexMap.descendingMap().entrySet().iterator();
			}

			while (iterator.hasNext()) {
				
				if(isDone){
					break;
				}
				
				Map.Entry entry = (Entry) iterator.next();

				// if multiple rows have same index value(clustered)
				List<String> toOrderByElement2 = (ArrayList<String>) entry.getValue();

				if (toOrderByElement2.size() > 1 && orderByElementsList.size() > 1) {
					// 2 order by column criteria, then sort the cluster based
					// on second column
					toOrderByElement2 = MyCreateTable.sortOnIndex2(orderByElementsList.get(1), toOrderByElement2);

					// print cluster in rev when desc(desc = 2)
					if (orderByElementsSortOrder.get(orderByElementsList.get(1)) == 2) {
						for (int i = toOrderByElement2.size() - 1; i >= 0; i--) {
							newRow = primaryKeyIndex.get(toOrderByElement2.get(i));
							processReadFromFile(ret);
						}

					} else {
						for (String rowString : toOrderByElement2) {
							// read new row from (PK,entire row ) map
							newRow = primaryKeyIndex.get(rowString);
							processReadFromFile(ret);
						}
					}
				} else {

					// clustered index
					for (String rowString : toOrderByElement2) {
						// read new row from (PK,entire row ) map
						newRow = primaryKeyIndex.get(rowString);
						processReadFromFile(ret);
					}
				}

			}
			

			if (numAggFunc > 0)
				printAggregateResult();
		}
	}
	
	private static String getNext(StringTokenizer st){  
	    String value = st.nextToken();
	    if ("\\|".equals(value))  
	        value = null;  
	    else if (st.hasMoreTokens())  
	        st.nextToken();  
	    return value;  
	}

	public static void processReadFromFile(PrimitiveValue ret) throws SQLException {

		if (innerSelects.size() != 0) {
			outermost = false;
		}

		/* read line from csv file */
		/* values array have individual column values from the file */
		
		int j = 0;
		StringTokenizer st = new StringTokenizer(newRow, "\\|", true);
		values = new String[Main.columnDataTypeMapping.size()];
		while (st.hasMoreTokens()) {
			values[j] = getNext(st);
			j++;
		}
		
		//values = newRow.split("\\|", -1);

		/* where clause evaluation */
		if (!(e == null)) {

			if (eval.eval(e).toBool()) {
				if (numAggFunc > 0) {
					computeAggregate();
				} else {
					printToConsole();
				}
			} else {
				newRow = ""; // making this "" as it shouldn't be passed on to
								// outer selects
			}
		} else {
			if (numAggFunc > 0) {
				computeAggregate();
			} else {
				printToConsole();
			}
		}

		/*
		 * row is returned from the file, so pass it on to outer select
		 * statements
		 */
		if (!outermost) {
			if (!newRow.equals("")) {

				/*
				 * innerSelects has a list of all the inner/nested select
				 * statements NOT the outermost/main select statement
				 */
				for (int i = innerSelects.size() - 2; i >= 0 && !newRow.equals(""); i--) {

					reinitializeValues();
					pq.processInBetweenSelect(innerSelects.get(i).toString());
				}

				/*
				 * row is returned till the end
				 */
				if (!newRow.equals("")) {
					outermost = true;

					// reinitializeValues();

					pq.processInBetweenSelect(query.toString());
				}
			}
		}

	}

	/*
	 * sum = 1 min = 2 max = 3 avg = 4 count = 5
	 */
	public static void printAggregateResult() {

		StringBuilder sb = new StringBuilder();
		if (groupByElementsList == null) {
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

			if (sb.length() > 0)
				sb.setLength(sb.length() - 1);

			System.out.println(sb);
		} else {

			// System.out.println("sel:: " +
			// Arrays.toString(selectItemsAsObject));

			// for(Entry<String, Double[]> g : aggGroupByMap.entrySet()){
			// System.out.println(g.getKey() + " :: " +
			// Arrays.toString(g.getValue()));
			// }

			List<String> tempList = new ArrayList<String>();
			for (Column c : groupByElementsList) {
				tempList.add(c.toString());
			}

			int[] pos = new int[tempList.size()];
			int j;

			for (j = 0; j < pos.length; j++)
				pos[j] = -1;

			for (j = 0; j < selectItemsAsObject.length; j++) {
				if (selectItemsAsObject[j] != null) {
					pos[j] = tempList.indexOf(selectItemsAsObject[j].toString());
				}
			}

			// System.out.println(Arrays.toString(pos));

			// get the sel columns
			for (Entry<String, Double[]> a : aggGroupByMap.entrySet()) {
				sb = new StringBuilder();
				String[] sitems = a.getKey().split(":");
				// System.out.println(Arrays.toString(sitems));

				// ignore the 0th index
				for (j = 0; j < pos.length && pos[j] != -1; j++) {
					if (pos.length == 1) {
						sb.append(sitems[pos[j]]);
					} else {
						sb.append(sitems[pos[j] + 1]);
					}
					sb.append("|");
				}

				// now get the agg results
				for (int i = 0; i < numAggFunc; i++) {

					if (aggNo[i] == 1) {
						sb.append(a.getValue()[0]);
						sb.append('|');
					} else if (aggNo[i] == 2) {
						sb.append(a.getValue()[1]);
						sb.append('|');
					} else if (aggNo[i] == 3) {
						sb.append(a.getValue()[2]);
						sb.append('|');
					} else if (aggNo[i] == 4) {
						sb.append(a.getValue()[3]);
						sb.append('|');
					} else if (aggNo[i] == 5) {
						sb.append(a.getValue()[4]);
						sb.append('|');
					}

				}
				if (sb.length() > 0)
					sb.setLength(sb.length() - 1);
				System.out.println(sb);
			}

		}
	}

	/*
	 * sum = 1 min = 2 max = 3 avg = 4 count = 5
	 */

	public static void computeAggregate() throws SQLException {

		print = false;
		aggPrint = true;

		boolean countOnce = false;
		boolean sumOnce = false;

		if (groupByElementsList != null) {

			String key = "";
			if (groupByElementsList.size() == 1) {
				int idx = columnOrderMapping.get(groupByElementsList.get(0).toString());
				key = values[idx];
			} else {
				for (int i = 0; i < groupByElementsList.size(); i++) {
					int idx = columnOrderMapping.get(groupByElementsList.get(i).toString());
					key = key + ":" + values[idx];
				}
			}

			for (int i = 0; i < numAggFunc; i++) {

				if ((aggNo[i] == 5 || aggNo[i] == 4) && (countOnce == false)) { // count
																				// or
																				// avg

					countOnce = true;
					if (groupByElementsList.size() != 0) {

						if (!aggGroupByMap.containsKey(key)) {

							Double[] arr = { 0.0, (double) Integer.MAX_VALUE, (double) Integer.MIN_VALUE, 0.0, 1.0 };
							aggGroupByMap.put(key, arr);

						} else {
							Double[] arr = aggGroupByMap.get(key);
							arr[4] = arr[4] + 1;
							aggGroupByMap.put(key, arr);
						}

					}
				}

				if (aggNo[i] != 5) {
					aggExpr = (Expression) aggExprs[i];
					answer = computeExpression();

					if ((aggNo[i] == 1 || aggNo[i] == 4) && (sumOnce == false)) {

						sumOnce = true;
						if (groupByElementsList.size() != 0) {

							if (!aggGroupByMap.containsKey(key)) {

								Double[] arr = { answer.toDouble(), (double) Integer.MAX_VALUE,
										(double) Integer.MIN_VALUE, 0.0, 0.0 };
								aggGroupByMap.put(key, arr);

							} else {
								Double[] arr = aggGroupByMap.get(key);
								arr[0] = arr[0] + answer.toDouble();
								aggGroupByMap.put(key, arr);
							}

						}

					} else if (aggNo[i] == 2) {

						if (groupByElementsList.size() != 0) {

							if (!aggGroupByMap.containsKey(key)) {

								Double[] arr = { answer.toDouble(), answer.toDouble(), answer.toDouble(), 0.0, 0.0 };
								aggGroupByMap.put(key, arr);

							} else {
								Double[] arr = aggGroupByMap.get(key);
								if (answer.toDouble() < arr[1]) {
									arr[1] = answer.toDouble();
								}
								aggGroupByMap.put(key, arr);
							}

						}

					} else if (aggNo[i] == 3) {

						if (groupByElementsList.size() != 0) {

							if (!aggGroupByMap.containsKey(key)) {

								Double[] arr = { answer.toDouble(), (double) answer.toDouble(), answer.toDouble(), 0.0,
										0.0 };
								aggGroupByMap.put(key, arr);

							} else {
								Double[] arr = aggGroupByMap.get(key);
								if (answer.toDouble() > arr[2]) {
									arr[2] = answer.toDouble();
								}
								aggGroupByMap.put(key, arr);
							}

						}

					}
				}
				// if avg
				if (aggNo[i] == 4) {

					Double[] arr = aggGroupByMap.get(key);
					arr[3] = arr[0] / arr[4];
					aggGroupByMap.put(key, arr);
				}

			}
		} else {

			print = false;
			aggPrint = true;
			boolean co = false;

			for (int i = 0; i < numAggFunc; i++) {
				if (aggNo[i] == 5) {
					if (co == false) {
						aggCount++;
						co = true;
					}

				} else {
					aggExpr = (Expression) aggExprs[i];
					answer = computeExpression();

					if (aggNo[i] == 1) {
						aggSum += answer.toDouble();
					} else if (aggNo[i] == 2) {
						if (answer.toDouble() < aggMin) {
							aggMin = answer.toDouble();
						}
					} else if (aggNo[i] == 3) {
						if (answer.toDouble() > aggMax) {
							aggMax = answer.toDouble();
						}

					} else if (aggNo[i] == 4) {
						avgCount++;
						avgTotal += answer.toDouble();
					}
				}

			}
		}

	}

	public static void printToConsole() throws SQLException {

		if (selectStar == true) {
			if (outermost && ((limit >= 1 && count < limit) || limit == -1)) {
				if (!newRow.equals("")) {
					System.out.println(newRow);
					count++;
				}
				if (innerSelects.size() != 0) {
					outermost = false;
				}

			}
			//System.out.println("count and limit:: " + count + " : " + limit);
			if(count >= limit){
				//System.out.println("------------");
				isDone = true;
			}
		} else {
			sbuilder = new StringBuilder();
			for (int i = 0; i < selCols; i++) {
				SelectExpressionItem sitem = (SelectExpressionItem) selectItemsAsObject[i];

				if (selExp instanceof Addition || selExp instanceof Subtraction || selExp instanceof Multiplication
						|| selExp instanceof Division) {

					Eval eval = new Eval() {
						public PrimitiveValue eval(Column c) {

							int idx = columnOrderMapping.get(c.toString());
							String ptype = columnDataTypeMapping.get(c.toString());
							return getReturnType(SQLDataType.valueOf(ptype), values[idx]);
						}
					};

					result = eval.eval(selExp);
					sbuilder.append(result);

				} else {
					int idx = columnOrderMapping.get(sitem.toString());
					sbuilder.append(values[idx]);
				}

				if (i != selCols - 1)
					sbuilder.append("|");
			}

			if (outermost && ((limit >= 1 && count < limit) || limit == -1)) {
				if (!newRow.equals("")) {
					System.out.println(sbuilder.toString());
					count++;
				}
				if (innerSelects.size() != 0) {
					outermost = false;
				}

			} else {
				newRow = sbuilder.toString();
			}
			// System.out.println("new row: " + newRow);
		}

	}

	public static Eval eval = new Eval() {
		public PrimitiveValue eval(Column c) {

			// System.out.println("-eval-" + c);

			int idx = columnOrderMapping.get(c.toString());
			String ptype = columnDataTypeMapping.get(c.toString());

			// return getReturnType(ptype, values[idx]);
			// System.out.println(ptype);
			return getReturnType(SQLDataType.valueOf(ptype), values[idx]);
		}
	};
	static PrimitiveValue expResult = null;

	private static PrimitiveValue computeExpression() throws SQLException {
		expResult = eval.eval(aggExpr);
		return expResult;
	}

}
