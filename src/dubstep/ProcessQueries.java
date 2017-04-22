package dubstep;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ProcessQueries {
	
	public static boolean isInstanceOfSelect(SubSelect ss) {
		if (ss instanceof SubSelect) {
			return true;
		} else {
			return false;
		}
	}

	public void populateInnerSelectStatements(SubSelect subQuery) {

		while (isInstanceOfSelect(subQuery)) {
			Main.innerSelects.add(subQuery);
			Main.plainSelect = (PlainSelect) subQuery.getSelectBody();

			if (!(Main.plainSelect.getFromItem() instanceof Table)) {
				subQuery = (SubSelect) Main.plainSelect.getFromItem();
			} else {
				break;
			}
		}
	}
	
	public void processInBetweenSelect(String selectQuery) {

		if (!Main.outermost) {
			selectQuery = selectQuery.substring(1, selectQuery.length() - 1);
		}

		StringReader ip = new StringReader(selectQuery);
		CCJSqlParser parser = new CCJSqlParser(ip);
		Statement query = null;
		try {
			query = parser.Statement();
		} catch (ParseException e1) {
			e1.printStackTrace();
		}

		Main.select = (Select) query;
		Main.plainSelect = (PlainSelect) Main.select.getSelectBody();

		Expression innerWhere = Main.plainSelect.getWhere();

		getSelectItemsList();

		try {
			processPassedResult(innerWhere);
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	public static void processPassedResult(Expression innerWhere) throws SQLException {

		// System.out.println("in processPassedResult with where: " +
		// innerWhere);

		PrimitiveValue ret = null;

		if (!(innerWhere == null)) {
			ret = Main.eval.eval(innerWhere);
			if ("TRUE".equals(ret.toString())) {
				if (Main.numAggFunc > 0) {
					Main.computeAggregate();
				} else {
					Main.printToConsole();
				}
			}else{
				Main.newRow = "";
				Main.values = null;
			}
		} else {
			if (Main.numAggFunc > 0) {
				Main.computeAggregate();
			} else {
				Main.printToConsole();
			}
		}

	}
		
	public void processInnermostSelect(String tblName) {
		String temp = "";
		Statement query_original = Main.query;
		Statement query = null;
		if (Main.innerSelects.size() > 0) {
			temp = Main.innerSelects.get(Main.innerSelects.size() - 1).toString();
			temp = temp.substring(1, temp.length() - 1);
			StringReader ip = new StringReader(temp);
			CCJSqlParser parser = new CCJSqlParser(ip);

			try {
				query = parser.Statement();
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} else {
			query = query_original;
		}
		Main.select = (Select) query;
		Main.plainSelect = (PlainSelect) Main.select.getSelectBody();

		//Main.myTableName = Main.plainSelect.getFromItem().toString();
		Main.tableData = Main.tableMapping.get(tblName);

		Main.columnOrderMapping = Main.tableData.getColumnOrderMapping();
		Main.columnDataTypeMapping = Main.tableData.getColumnDataTypeMapping();

		getSelectItemsList();

		try {
			Main.readFromFile();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void getSelectItemsList() {

		Main.selectItemsAsObject = new SelectItem[Main.plainSelect.getSelectItems().size()];
		Main.aggNo = new int[Main.plainSelect.getSelectItems().size()];
		Main.aggExprs = new Expression[Main.plainSelect.getSelectItems().size()];
		Main.aggAlias = new String[Main.plainSelect.getSelectItems().size()];
		
		int i = 0, j = 0;
		for (SelectItem sitem : Main.plainSelect.getSelectItems()) {
			if (sitem instanceof AllColumns) {
				Main.selectStar = true;
			} else {
				Main.selExp = ((SelectExpressionItem) sitem).getExpression();
				Main.ssitem = sitem.toString();

				if (Main.selExp instanceof Function) {
					Main.aggName = ((Function) Main.selExp).getName();
					Main.aggFunctions = Main.AggFunctions.valueOf(Main.aggName);
					Main.aggNo[i] = Main.getAggNo(Main.aggFunctions);
					Main.aggAlias[i] = ((SelectExpressionItem)sitem).getAlias().toString();

					if (Main.aggNo[i] != 5) {
						//System.out.println("--" + Main.selExp);
						Main.aggExprs[i] = (Expression)((Function) Main.selExp).getParameters().getExpressions().get(0);
						//System.out.println(Main.aggExprs[i]);
					}
					i++;
				} else {
					Main.selectItemsAsObject[j] = sitem;
					j++;
					Main.selectItemsMap.put(sitem.toString(), null);
				}
			}
		}
		Main.selCols = j;
		Main.numAggFunc = i;
	}

}
