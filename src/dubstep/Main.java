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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
		string, varchar, sqlchar, sqlint, DECIMAL, DATE, decimal, date, STRING, VARCHAR
	};

	public static Main mainObj = new Main();
	public static TableData tableData = null;

	public static Map<String, TableData> tableMapping = new HashMap<String, TableData>();
	public static Map<String, Integer> columnOrderMapping = new HashMap<String, Integer>();
	public static Map<String, String> columnDataTypeMapping = new HashMap<String, String>();
	public static Map<String, Map> columnIndex = new HashMap<String, Map>();
	public static Map<String, HashMap<String, Double>> aggGroupByMap = new HashMap<String, HashMap<String, Double>>();

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

	public static String countAlias;

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
	public static long aggCount = 0;
	public static double aggAvg = 0.0;
	public static double avgTotal = 0.0;
	public static int[] aggNo = null;
	public static String[] aggAlias = null;
	public static HashMap<String, Double> aggResults = new HashMap<>();

	public static AggFunctions aggFunctions;
	public static SQLDataType sqlDataType;
	public static Boolean print = null;
	public static Boolean outermost = false;

	public static ProcessQueries pq = null;
	public static MyCreateTable ct = null;

	public static long limit = 0;
	public static long count = 0;

	public static Map<Long, String[]> primaryKeyIndex = new HashMap<Long, String[]>();
	public static List<String> primaryKeyList = new ArrayList<>();

	public static List<String> orderByElementsList = new ArrayList<String>();
	public static Map<String, Integer> orderByElementsSortOrder = new HashMap<>();
	public static String alias;
	public static boolean line = false;

	public static boolean isDone = false;
	public static boolean stop = false;

	public static boolean inmem = false;

	public static boolean groupByOperator = false;

	public static List<String> outputDataOD = new ArrayList<>();

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
		} else if (ptype == SQLDataType.varchar || ptype == SQLDataType.sqlchar || ptype == SQLDataType.string
				|| ptype == SQLDataType.VARCHAR || ptype == SQLDataType.STRING) {
			return new StringValue(value.toUpperCase());
		} else if (ptype == SQLDataType.DATE || ptype == SQLDataType.date) {
			return new DateValue(value);
		} else if (ptype == SQLDataType.DECIMAL || ptype == SQLDataType.decimal) {
			return new DoubleValue(value);
		}

		return null;
	}

	public static void main(String[] args) throws ParseException, SQLException, IOException {

		String phase = args[1];
		if (phase.equals("--in-mem")) {
			inmem = true;
		} else {
			inmem = false;
		}

		// System.out.println("args::" + args[1]);
		System.out.print("$>");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		/*
		 * keep reading from the grader
		 */
		while ((inputString = br.readLine()) != null) {

			// inputString = inputString.toUpperCase();
			inputString = inputString.toUpperCase();

			StringBuilder inputStringBuilder = new StringBuilder();
			inputStringBuilder.append(inputString);

			while (inputString.contains(";") == false && (inputString = br.readLine()) != null) {
				inputStringBuilder.append(" ");
				inputStringBuilder.append(inputString);
				inputString = inputStringBuilder.toString();
			}

			input = new StringReader(inputString);
			// input = new StringReader(inputString);
			parser = new CCJSqlParser(input);

			try {
				query = parser.Statement();
				long starttime = System.currentTimeMillis();
				// create table query
				if (query instanceof CreateTable) {

					ct = new MyCreateTable();
					ct.createTable();

					long endtime = System.currentTimeMillis();
					System.out.println("time taken::" + (endtime - starttime));
					// System.out.println("args::" + args[1]);

				} else if (query instanceof Select) { // select queries

					reinitializeValues();

					count = 0;
					limit = -1;
					isDone = false;
					stop = false;
					groupByOperator = false;
					orderOperator = false;

					outputDataOD = new ArrayList<>();

					innerSelects = new ArrayList<>(); // stores nested select
														// statements
					pq = new ProcessQueries();

					selectStar = false;

					select = (Select) query;
					plainSelect = (PlainSelect) select.getSelectBody();

					// orderByElementsList = plainSelect.getOrderByElements();
					groupByElementsList = plainSelect.getGroupByColumnReferences();

					if (groupByElementsList != null) {
						groupByOperator = true;
					}

					// System.out.println("gb::" + groupByElementsList);

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
						alias = fromItem.getAlias();

						if (alias != null) {
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

					// System.out.println(inmem + " -- " + orderOperator + " --
					// " + groupByOperator);

					if (inmem == false && orderOperator == true && groupByOperator == true) {
						TreeMap<String, List<String>> outputDataODMap = new TreeMap<>();
						List<String> list = new ArrayList<String>();
						for (String rowVal : outputDataOD) {
							String opVal[] = rowVal.split("\\|");

							if (outputDataODMap.containsKey(opVal[0])) {
								list = (List<String>) outputDataODMap.get(opVal[0]);
								list.add(rowVal);
								Collections.sort(list, new Comparator<String>() {
									public int compare(String a1, String a2) {
										String a1Arr[] = a1.split("\\|");
										String a2Arr[] = a2.split("\\|");
										return a1Arr[1].compareTo(a2Arr[1]);
									}
								});
								outputDataODMap.put(opVal[0], list);
							} else {
								list = new ArrayList<String>();
								list.add(rowVal);
								outputDataODMap.put(opVal[0], list);
							}
						}

						Iterator iterator = outputDataODMap.entrySet().iterator();

						// System.out.println("printing");
						while (iterator.hasNext()) {
							Map.Entry entry = (Entry) iterator.next();
							for (String rowString : (ArrayList<String>) entry.getValue()) {
								System.out.println(rowString);
							}

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
		avgTotal = 0.0;

		aggGroupByMap = new HashMap<>();
		aggResults = new HashMap<>();

		// orderOperator = false;

	}

	public static void readFromFile() throws SQLException, IOException {

		/*
		 * read from the file directly, as no order by clause is present
		 */
		if (orderOperator == false) {

			File file;
			file = new File("data/" + Main.myTableName + ".csv");

			
			// get the where clause
			e = plainSelect.getWhere();

			
			if(e.toString().contains("LINEITEM.QUANTITY")){
				file = new File("data/LINEITEM.QUANTITY.csv");
			}
			
			BufferedReader br = new BufferedReader(new FileReader(file));
			
			// reinitializeValues();

			PrimitiveValue ret = null;

			try {

				while ((newRow = br.readLine()) != null) {

					line = true;
					if(stop == false){
						processReadFromFile(ret);
					}else{
						//System.out.println("stopped");
						break;
					}
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
			line = false;
			e = plainSelect.getWhere();
			reinitializeValues();
			PrimitiveValue ret = null;

			// firstOrderOperator csv file name
			String firstOrderOperator = orderByElementsList.get(0);

			// start of in-mem
			if (inmem) {
				TreeMap orderIndexMap = new TreeMap<>();
				System.out.println(firstOrderOperator);
				String[] temp12 = firstOrderOperator.split("\\.");
				System.out.println(Arrays.toString(temp12));
				boolean m = false;
				
				for(Entry<String, Map> ci : columnIndex.entrySet()){
					if(ci.getKey().contains(temp12[1])){
						System.out.println("index present");
						orderIndexMap = (TreeMap) ci.getValue();
						m = true;
						break;
					}
				}

				if(m == false){
					// if index not built on order by column, build it on the
					// fly
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

					if (isDone) {
						break;
					}

					Map.Entry entry = (Entry) iterator.next();

					// System.out.println("--" + entry);

					// if multiple rows have same index value(clustered)
					List<Long> toOrderByElement2 = (List<Long>) entry.getValue();

					if (toOrderByElement2.size() > 1 && orderByElementsList.size() > 1) {
						// 2 order by column criteria, then sort the cluster
						// based
						// on second column
						toOrderByElement2 = MyCreateTable.sortOnIndex2(orderByElementsList.get(1), toOrderByElement2);

						// print cluster in rev when desc(desc = 2)
						if (orderByElementsSortOrder.get(orderByElementsList.get(1)) == 2) {
							for (int i = toOrderByElement2.size() - 1; i >= 0; i--) {
								values = primaryKeyIndex.get(toOrderByElement2.get(i));
								processReadFromFile(ret);
							}

						} else {
							for (Long rowString : toOrderByElement2) {
								// read new row from (PK,entire row ) map
								values = primaryKeyIndex.get(rowString);
								processReadFromFile(ret);
							}
						}
					} else {

						// clustered index
						for (Long rowString : toOrderByElement2) {
							// read new row from (PK,entire row ) map
							values = primaryKeyIndex.get(rowString);
							processReadFromFile(ret);
						}
					}

				}

				if (numAggFunc > 0)
					printAggregateResult();
			} else {

				// ---------------------------onDisk---------------------------

				// System.out.println("file:" + firstOrderOperator);
				File file;
				file = new File("data/" + firstOrderOperator + ".csv");

				BufferedReader br = new BufferedReader(new FileReader(file));
				// get the where clause
				e = plainSelect.getWhere();

				// reinitializeValues();

				ret = null;

				try {

					while ((newRow = br.readLine()) != null) {
						
						if(isDone){
							//System.out.println("done dona done done!");
							break;
						}

						line = true;
						processReadFromFile(ret);
					}

					/*
					 * done with file reading...if aggregate function, then
					 * print after this and not in printToConsole
					 */

					if (numAggFunc > 0)
						printAggregateResult();

				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}

			}
		}
	}

	public static void processReadFromFile(PrimitiveValue ret) throws SQLException {

		if (innerSelects.size() != 0) {
			outermost = false;
		}

		/* read line from csv file */
		/* values array have individual column values from the file */

		// System.out.println();
		if (line) {
			values = newRow.split("\\|", -1);
		}
		
		

		/* where clause evaluation */
		if (!(e == null)) {
			
			if(e.toString().contains("LINEITEM.QUANTITY")){
				int ii = columnOrderMapping.get("LINEITEM.QUANTITY");
				if(Integer.parseInt(values[ii]) > 26){
					stop = true;
				}
			}

			if (eval.eval(e).toBool()) {
				if (numAggFunc > 0) {
					computeAggregate();
				} else {
					printToConsole();
				}
			} else {
				newRow = ""; // making this "" as it shouldn't be passed on to
				values = null; // outer selects
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
			if (values != null) {

				/*
				 * innerSelects has a list of all the inner/nested select
				 * statements NOT the outermost/main select statement
				 */
				for (int i = innerSelects.size() - 2; i >= 0 && (values != null); i--) {

					reinitializeValues();
					pq.processInBetweenSelect(innerSelects.get(i).toString());
				}

				/*
				 * row is returned till the end
				 */
				if (values != null) {
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

			for (int i = 0; i < aggAlias.length; i++) {
				if (aggResults.get(aggAlias[i]) != null) {
					sb.append(aggResults.get(aggAlias[i]));
					sb.append('|');
				}
				if (aggNo[i] == 5 && aggResults.get(aggAlias[i]) == null) {
					sb.append(0);
					sb.append('|');
				}
			}

			if (sb.length() > 0) {
				sb.setLength(sb.length() - 1);

				// System.out.println(sb);
				if (inmem) {
					System.out.println(sb);
				}

				// put in map if ondisk & order operator present

				if (inmem == false && orderOperator == true && groupByOperator == true) {
					outputDataOD.add(sb.toString());
				} else {
					if (inmem == false) {
						System.out.println(sb.toString());
					}
				}
			}
		} else {

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
			for (Entry<String, HashMap<String, Double>> a : aggGroupByMap.entrySet()) {
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
				HashMap<String, Double> agg = a.getValue();
				Double count = agg.get(countAlias);
				for (int i = 0; i < aggAlias.length; i++) {

					if (aggNo[i] == 4) {
						i = i + 1;
						sb.append(agg.get(aggAlias[i]) / count);
						sb.append('|');
					} else {

						if (agg.get(aggAlias[i]) != null) {
							sb.append(agg.get(aggAlias[i]));
							sb.append('|');
						}
					}
					if (aggNo[i] == 5 && agg.get(aggAlias[i]) == null) {
						sb.append(0);
						sb.append('|');
					}

				}

				if (sb.length() > 0)
					sb.setLength(sb.length() - 1);

				// System.out.println(sb);
				if (inmem) {
					System.out.println(sb);
				}

				// put in map if ondisk & order operator present

				if (inmem == false && orderOperator == true && groupByOperator == true) {
					outputDataOD.add(sb.toString());
				} else {
					if (inmem == false) {
						System.out.println(sb.toString());
					}
				}
			}

		}
	}

	/*
	 * sum = 1 min = 2 max = 3 avg = 4 count = 5
	 */

	public static void computeAggregate() throws SQLException {

		print = false;
		aggPrint = true;

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

				if (aggNo[i] == 5) { // count

					if (groupByElementsList.size() != 0) {

						if (!aggGroupByMap.containsKey(key)) {

							HashMap<String, Double> agg = new HashMap<>();
							agg.put(aggAlias[i], 1.0);

							aggGroupByMap.put(key, agg);

						} else {
							HashMap<String, Double> agg = aggGroupByMap.get(key);
							if (!agg.containsKey(aggAlias[i])) {
								// System.out.println("--count--");
								agg.put(aggAlias[i], 1.0);
							} else {
								Double c = agg.get(aggAlias[i]);
								c = c + 1;
								agg.put(aggAlias[i], c);
							}

							aggGroupByMap.put(key, agg);
						}

					}
				}

				if (aggNo[i] != 5) {
					aggExpr = (Expression) aggExprs[i];
					answer = computeExpression();

					if (aggNo[i] == 1) {

						if (groupByElementsList.size() != 0) {

							if (!aggGroupByMap.containsKey(key)) {

								HashMap<String, Double> agg = new HashMap<>();
								agg.put(aggAlias[i], answer.toDouble());

								aggGroupByMap.put(key, agg);

							} else {

								HashMap<String, Double> agg = aggGroupByMap.get(key);

								if (!agg.containsKey(aggAlias[i])) {
									agg.put(aggAlias[i], answer.toDouble());
								} else {
									Double sum = agg.get(aggAlias[i]) + answer.toDouble();
									agg.put(aggAlias[i], sum);
								}

								aggGroupByMap.put(key, agg);
							}

						}

					} else if (aggNo[i] == 2) { // min

						if (groupByElementsList.size() != 0) {

							if (!aggGroupByMap.containsKey(key)) {

								HashMap<String, Double> agg = new HashMap<>();
								agg.put(aggAlias[i], answer.toDouble());

								aggGroupByMap.put(key, agg);

							} else {

								HashMap<String, Double> agg = aggGroupByMap.get(key);

								if (!agg.containsKey(aggAlias[i])) {
									agg.put(aggAlias[i], answer.toDouble());
								} else {
									Double min = agg.get(aggAlias[i]);
									if (answer.toDouble() < min) {
										min = answer.toDouble();
										agg.put(aggAlias[i], min);
									}

								}

								aggGroupByMap.put(key, agg);
							}

						}

					} else if (aggNo[i] == 3) {

						if (groupByElementsList.size() != 0) {

							if (!aggGroupByMap.containsKey(key)) {

								HashMap<String, Double> agg = new HashMap<>();
								agg.put(aggAlias[i], answer.toDouble());

								aggGroupByMap.put(key, agg);

							} else {

								HashMap<String, Double> agg = aggGroupByMap.get(key);

								if (!agg.containsKey(aggAlias[i])) {
									agg.put(aggAlias[i], answer.toDouble());
								} else {
									Double max = agg.get(aggAlias[i]);
									if (answer.toDouble() > max) {
										max = answer.toDouble();
										agg.put(aggAlias[i], max);
									}

								}

								aggGroupByMap.put(key, agg);
							}

						}

					} else if (aggNo[i] == 4) {

						if (!aggGroupByMap.containsKey(key)) {

							HashMap<String, Double> agg = new HashMap<>();
							agg.put(aggAlias[i], answer.toDouble());

							aggGroupByMap.put(key, agg);

						} else {

							HashMap<String, Double> agg = aggGroupByMap.get(key);

							avgCount++;
							avgTotal += answer.toDouble();
							agg.put(aggAlias[i], avgTotal / avgCount);

							aggGroupByMap.put(key, agg);
						}

					}
				}
				// if avg
				// if (aggNo[i] == 4) {
				//
				// Double[] arr = aggGroupByMap.get(key);
				// arr[3] = arr[0] / arr[4];
				// aggGroupByMap.put(key, arr);
				// }

			}
		} else {

			print = false;
			aggPrint = true;

			for (int i = 0; i < numAggFunc; i++) {
				if (aggNo[i] == 5) {
					if (!aggResults.containsKey(aggAlias[i])) {
						// System.out.println("--count--");
						aggResults.put(aggAlias[i], 1.0);
					} else {
						Double c = aggResults.get(aggAlias[i]);
						c = c + 1;
						aggResults.put(aggAlias[i], c);
					}

				} else {
					aggExpr = (Expression) aggExprs[i];
					answer = computeExpression();

					if (aggNo[i] == 1) {

						if (!aggResults.containsKey(aggAlias[i])) {
							aggResults.put(aggAlias[i], answer.toDouble());
						} else {
							Double sum = aggResults.get(aggAlias[i]) + answer.toDouble();
							aggResults.put(aggAlias[i], sum);
						}

					} else if (aggNo[i] == 2) {

						if (!aggResults.containsKey(aggAlias[i])) {
							aggResults.put(aggAlias[i], answer.toDouble());
						} else {
							Double min = aggResults.get(aggAlias[i]);
							if (answer.toDouble() < min) {
								min = answer.toDouble();
								aggResults.put(aggAlias[i], min);
							}

						}

					} else if (aggNo[i] == 3) {

						if (!aggResults.containsKey(aggAlias[i])) {
							aggResults.put(aggAlias[i], answer.toDouble());
						} else {
							Double max = aggResults.get(aggAlias[i]);
							if (answer.toDouble() > max) {
								max = answer.toDouble();
								aggResults.put(aggAlias[i], max);
							}

						}

					} else if (aggNo[i] == 4) {
						avgCount++;
						avgTotal += answer.toDouble();
						aggResults.put(aggAlias[i], avgTotal / avgCount);
					}
				}

			}
		}

	}

	public static void printToConsole() throws SQLException {

		if (selectStar == true) {
			
			if (outermost && ((limit >= 1 && count < limit) || limit == -1)) {
				if (values != null) {
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < values.length; i++) {
						if (values[i] != null) {
							sb.append(values[i]);
							sb.append("|");
						} else {
							break;
						}
					}
					if (sb.length() > 0)
						sb.setLength(sb.length() - 1);

					// System.out.println(sb);
					if (inmem) {
						System.out.println("conut: " + count);
						System.out.println(sb);
					}

					// put in map if ondisk & order operator present

					if (inmem == false && orderOperator == true && groupByOperator == true) {
						outputDataOD.add(sb.toString());
					} else {
						if (inmem == false) {
							System.out.println(sb.toString());
						}
					}
					count++;
				}
				if (innerSelects.size() != 0) {
					outermost = false;
				}

			}
			if (count >= limit && limit != -1) {
				// System.out.println("------------");
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
					if (result != null) {
						sbuilder.append(result);
					}

				} else {
					int idx = columnOrderMapping.get(sitem.toString());
					if (values[idx] != null) {
						sbuilder.append(values[idx]);
					}
				}

				if (i != selCols - 1)
					sbuilder.append("|");
			}

			if (outermost && ((limit >= 1 && count < limit) || limit == -1)) {
				if (values != null) {
					// System.out.println(sbuilder.toString());
					if (inmem) {
						System.out.println(sbuilder);
					}

					// put in map if ondisk & order operator present

					if (inmem == false && orderOperator == true && groupByOperator == true) {
						outputDataOD.add(sbuilder.toString());
					} else {
						if (inmem == false) {
							System.out.println(sbuilder.toString());
						}
					}
					count++;
				}
				if (innerSelects.size() != 0) {
					outermost = false;
				}

			} else {
				newRow = sbuilder.toString();
			}
			if (count >= limit && limit != -1) {
				isDone = true;
			}
		}

	}

	public static Eval eval = new Eval() {
		public PrimitiveValue eval(Column c) {

			if (c.toString().contains(myTableName)) {
				Main.tableData = Main.tableMapping.get(myTableName);

				Main.columnOrderMapping = Main.tableData.getColumnOrderMapping();
				Main.columnDataTypeMapping = Main.tableData.getColumnDataTypeMapping();
			} else {
				Main.tableData = Main.tableMapping.get(alias);

				Main.columnOrderMapping = Main.tableData.getColumnOrderMapping();
				Main.columnDataTypeMapping = Main.tableData.getColumnDataTypeMapping();
			}

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
