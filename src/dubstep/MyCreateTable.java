package dubstep;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class MyCreateTable {
	
	public static Index primaryKey;
	public static Index indexKey;
	public static List<String> primaryKeyList;
	public static List<String> indexKeyList; 

	public static void createTable() throws IOException {
		Main.table = (CreateTable) Main.query;
		List<Index> tableIndex = Main.table.getIndexes();
		
		Main.myTableName = Main.table.getTable().getName();
		primaryKeyList = new ArrayList<>();
		indexKeyList = new ArrayList<>();
		
		//System.out.println(tableIndex);
		if (tableIndex != null) {
			primaryKey = tableIndex.get(0);
			indexKey = tableIndex.get(1);

			for (String s : primaryKey.getColumnsNames()){
				String k = Main.myTableName + "." + s;
				primaryKeyList.add(k);
				indexKeyList.add(k);
			}
			
			for (String s : indexKey.getColumnsNames()){
				String k = Main.myTableName + "." + s;
				indexKeyList.add(k);
			}
			
			//System.out.println("pk::" + primaryKeyList);
			
			//primaryKeyList = primaryKey.getColumnsNames();

			//indexKeyList = indexKey.getColumnsNames();
			//System.out.println(indexKeyList);
		}
		
		Main.tableData = new TableData();
		Main.columnNames = Main.table.getColumnDefinitions();
		
		//System.out.println("cols:" + Main.columnNames);

		int i = 0;
		for (ColumnDefinition col : Main.columnNames) {
			Main.columnOrderMapping.put(Main.myTableName + "." +col.getColumnName(), i);

			String dtype = col.getColDataType().getDataType();
			if (dtype.equalsIgnoreCase("INT")) {
				dtype = "sqlint";
			} else if (dtype.equalsIgnoreCase("CHAR")) {
				dtype = "sqlchar";
			}

			Main.columnDataTypeMapping.put(Main.myTableName+ "." + col.getColumnName(), dtype);
			i++;
		}
		
		//System.out.println("cols:" + Main.columnOrderMapping);
		Main.tableData.setColumnDataTypeMapping(Main.columnDataTypeMapping);
		Main.tableData.setColumnOrderMapping(Main.columnOrderMapping);

		Main.tableMapping.put(Main.myTableName, Main.tableData);
		
		if (primaryKeyList != null) {
			makePrimaryMapping(primaryKeyList);
			for (String indexColumn : indexKeyList) {
				sortMyTable(indexColumn, primaryKeyList);
			}
		}

	}

	private static void makePrimaryMapping(List<String> primaryKeyList) throws FileNotFoundException {
		// TODO Auto-generated method stub
		//File file = new File(Main.myTableName + ".csv");
		File file = new File("data/" + Main.myTableName + ".csv");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String values[] = null;
		String newRow = "";
		String keyBuilder = "";
		int idx = -1;
		try {

			// keyBuilder = new StringBuilder();
			while ((newRow = br.readLine()) != null) {

				values = newRow.split("\\|", -1);
				// for(String col : primaryKeyList){
				// idx = Main.columnOrderMapping.get(col);
				// //keyBuilder.append(values[idx]);
				// keyBuilder = keyBuilder.concat(values[idx]) ;
				//
				// }

				if (primaryKeyList.size() == 1) {

					idx = Main.columnOrderMapping.get(primaryKeyList.get(0));
					Main.primaryKeyIndex.put(values[idx], newRow);
				} else {
					idx = Main.columnOrderMapping.get(primaryKeyList.get(0));
					keyBuilder = values[idx];
					idx = Main.columnOrderMapping.get(primaryKeyList.get(1));
					Main.primaryKeyIndex.put(keyBuilder.concat(values[idx]), newRow);
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void sortMyTable(String columnName, List<String> primaryKeyList) throws IOException {
		Map map = new TreeMap<>();
		String newRow = "";
		String keyBuilder = "";
		//File file = new File(Main.myTableName + ".csv");
		File file = new File("data/" + Main.myTableName + ".csv");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String values[] = null;
		List<String> list = null;
		int idx = -1;
		int idpk = -1;
		try {
			while ((newRow = br.readLine()) != null) {
				// System.out.println(newRow);
				values = newRow.split("\\|", -1);
				idx = Main.columnOrderMapping.get(columnName);
				String ptype = Main.columnDataTypeMapping.get(columnName);
				Main.SQLDataType ptype1 = Main.SQLDataType.valueOf(ptype);

				if (primaryKeyList.size() == 1) {

					idpk = Main.columnOrderMapping.get(primaryKeyList.get(0));
					newRow = values[idpk];
				} else {
					idpk = Main.columnOrderMapping.get(primaryKeyList.get(0));
					keyBuilder = values[idpk];
					idpk = Main.columnOrderMapping.get(primaryKeyList.get(1));
					newRow = keyBuilder.concat(values[idpk]);
				}

				if (ptype1 == Main.SQLDataType.sqlint) {

					int key = Integer.parseInt(values[idx]);
					if (map.containsKey(key)) {
						list = (List<String>) map.get(key);
						list.add(newRow);
						map.put(key, list);
					} else {
						list = new ArrayList<String>();
						list.add(newRow);
						map.put(key, list);
					}

				} else if (ptype1 == Main.SQLDataType.DECIMAL) {
					Double key = Double.parseDouble(values[idx]);
					if (map.containsKey(key)) {
						list = (List<String>) map.get(key);
						list.add(newRow);
						map.put(key, list);
					} else {
						list = new ArrayList<String>();

						list.add(newRow);
						map.put(key, list);
					}
				} else {
					// (ptype1 == Main.SQLDataType.string)

					String key = values[idx];
					if (map.containsKey(key)) {
						list = (List<String>) map.get(key);
						list.add(newRow);
						map.put(key, list);
					} else {
						list = new ArrayList<String>();
						list.add(newRow);
						map.put(key, list);
					}
				}

			}
			Main.columnIndex.put(columnName, map);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}
}
