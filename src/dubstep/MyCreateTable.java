
package dubstep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class MyCreateTable {

	public static Index primaryKey;
	public static Index indexKey;
	public static List<String> indexKeyList;

	public static void createTable() throws IOException {

		Main.table = (CreateTable) Main.query;
		List<Index> tableIndex = Main.table.getIndexes();
		List<String> indexKeyList = new ArrayList<>();
		Main.primaryKeyList = new ArrayList<>();

		Main.myTableName = Main.table.getTable().getName();
		Main.tableData = new TableData();
		Main.columnNames = Main.table.getColumnDefinitions();

		int i = 0;
		for (ColumnDefinition col : Main.columnNames) {
			Main.columnOrderMapping.put(Main.myTableName + "." + col.getColumnName(), i);

			String dtype = col.getColDataType().getDataType();
			if (dtype.equalsIgnoreCase("INT")) {
				dtype = "sqlint";
			} else if (dtype.equalsIgnoreCase("CHAR")) {
				dtype = "sqlchar";
			}

			Main.columnDataTypeMapping.put(Main.myTableName + "." + col.getColumnName(), dtype);
			i++;
		}
		Main.tableData.setColumnDataTypeMapping(Main.columnDataTypeMapping);
		Main.tableData.setColumnOrderMapping(Main.columnOrderMapping);

		Main.tableData.setPrimaryKeyList(Main.primaryKeyList);
		Main.tableMapping.put(Main.myTableName, Main.tableData);

		if (tableIndex != null) {
			for (Index indxValue : tableIndex) {
				if (indxValue.getType().length() == 5) {

					indexKeyList.addAll(indxValue.getColumnsNames());
				}

				else {
					for (String pkCol : indxValue.getColumnsNames()) {
						String pk = Main.myTableName + "." + pkCol;
						Main.primaryKeyList.add(pk);
					}
				}
			}
		}

		makePrimaryMapping(Main.primaryKeyList);
		for (String indexColumn : indexKeyList) {
			sortMyTable(Main.myTableName + "." + indexColumn, Main.primaryKeyList);
		}
	}

	private static void makePrimaryMapping(List<String> primaryKeyList) throws FileNotFoundException {

		File file;
		if (System.getProperty("user.home").contains("deepti")) {
			System.out.println("localq");
			file = new File(Main.myTableName + ".csv");
		} else {

			file = new File("data/" + Main.myTableName + ".csv");
		}

		BufferedReader br = new BufferedReader(new FileReader(file));
		String values[] = null;
		String newRow = "";
		String keyBuilder = "";
		int idx = -1;
		try {

			// keyBuilder = new StringBuilder();
			while ((newRow = br.readLine()) != null) {

				values = newRow.split("\\|", -1);
				

				if (primaryKeyList.size() != 0) {
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

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void sortMyTable(String columnName, List<String> primaryKeyList) throws IOException {
		Map map = new TreeMap<>();
		String newRow = "";
		String keyBuilder = "";

		File file;
		if (System.getProperty("user.home").contains("deepti")) {
			System.out.println("local");
			file = new File(Main.myTableName + ".csv");
		} else {

			file = new File("data/" + Main.myTableName + ".csv");
		}

		BufferedReader br = new BufferedReader(new FileReader(file));
		String values[] = null;
		List<String> list = null;
		int idx = -1;
		int idpk = -1;
		try {

			while ((newRow = br.readLine()) != null) {
				// System.out.println(newRow);
				values = newRow.split("\\|", -1);
				//System.out.println("colName::" + columnName);
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

				} else if (ptype1 == Main.SQLDataType.DECIMAL || ptype1 == Main.SQLDataType.decimal) {
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

	public static List<String> sortOnIndex2(String columnName, List<String> PKList) throws IOException {

		List<String> PKListSorted = new ArrayList<>();

		String newRow = "";
		String keyBuilder = "";
		Map map = new TreeMap<>();
		String values[] = null;
		List<String> list = null;
		int idx = -1;
		int idpk = -1;
		for (String rowString : PKList) {
			// System.out.println(newRow);
			newRow = Main.primaryKeyIndex.get(rowString);
			values = newRow.split("\\|", -1);
			idx = Main.columnOrderMapping.get(columnName);
			String ptype = Main.columnDataTypeMapping.get(columnName);
			Main.SQLDataType ptype1 = Main.SQLDataType.valueOf(ptype);
			if (ptype1 == Main.SQLDataType.sqlint) {
				int key = Integer.parseInt(values[idx]);
				if (map.containsKey(key)) {
					list = (List<String>) map.get(key);
					list.add(rowString);
					map.put(key, list);
				} else {
					list = new ArrayList<String>();
					list.add(rowString);
					map.put(key, list);
				}
			} else if (ptype1 == Main.SQLDataType.DECIMAL || ptype1 == Main.SQLDataType.decimal) {
				Double key = Double.parseDouble(values[idx]);
				if (map.containsKey(key)) {
					list = (List<String>) map.get(key);
					list.add(rowString);
					map.put(key, list);
				} else {
					list = new ArrayList<String>();

					list.add(rowString);
					map.put(key, list);
				}
			} else {
				String key = values[idx];
				if (map.containsKey(key)) {
					list = (List<String>) map.get(key);
					list.add(rowString);
					map.put(key, list);
				} else {
					list = new ArrayList<String>();
					
					
					list.add(rowString);
					map.put(key, list);
				}
			}
		}
		Iterator iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry entry = (Entry) iterator.next();
			for (String rowS : (ArrayList<String>) entry.getValue()) {
				PKListSorted.add(rowS);
			}
		}
		return PKListSorted;
	}
}
