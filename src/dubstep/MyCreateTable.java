package dubstep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class MyCreateTable {

	public static Index primaryKey;
	public static Index indexKey;
	public static List<String> indexKeyList;
	public static final String DELIM = "\\|";

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

		if (Main.inmem) {
			makePrimaryMapping(Main.primaryKeyList);

			for (String indexColumn : Main.primaryKeyList) {
				sortMyTable(indexColumn, Main.primaryKeyList);
			}

			for (String indexColumn : indexKeyList) {
				sortMyTable(Main.myTableName + "." + indexColumn, Main.primaryKeyList);
			}
		}else{
			
			// onDisk sort
			//pass index of column to sort
			//get index of "LINEITEM.RETURNFLAG", "LINEITEM.RECEIPTDATE" and "LINEITEM.LINESTATUS"
			
			int oIdx = Main.columnOrderMapping.get("LINEITEM.RETURNFLAG");
			System.out.println("oIdx::" + oIdx);
			
			ExternalSort.onDiskSort(oIdx, "LINEITEM.RETURNFLAG");
			
			oIdx = Main.columnOrderMapping.get("LINEITEM.RECEIPTDATE");
			System.out.println("oIdx::" + oIdx);
			
			ExternalSort.onDiskSort(oIdx, "LINEITEM.RECEIPTDATE");
		}
	}

	private static String getNext(StringTokenizer st){  
	    String value = st.nextToken();
	    if (DELIM.equals(value))  
	        value = null;  
	    else if (st.hasMoreTokens())  
	        st.nextToken();  
	    return value;  
	}
	    
	private static void makePrimaryMapping(List<String> primaryKeyList) throws IOException {

		File file;
//		if (System.getProperty("user.home").contains("deepti")) {
//			System.out.println("localq");
//			file = new File(Main.myTableName + ".csv");
//		} else {
//
//			file = new File("data/" + Main.myTableName + ".csv");
//		}

		file = new File("data/" + Main.myTableName + ".csv");
		//List<String> lines = Files.readAllLines(Paths.get(Main.myTableName + ".csv"), StandardCharsets.UTF_8);
		
		BufferedReader br = new BufferedReader(new FileReader(file));
		//String values[] = null;
		
		//System.out.println("size::" + Main.columnDataTypeMapping.size());
		String[] values = new String[Main.columnDataTypeMapping.size()];
		String newRow = "";
		String keyBuilder = "";
		int idx = -1;
		int idx1 = -1;
		int idx2 = -1;
		int i;
		try {
			
			if (primaryKeyList.size() == 1) {
				idx = Main.columnOrderMapping.get(primaryKeyList.get(0));
				
//				for(String ip : lines){
//					values = newRow.split("\\|", -1);
//					Main.primaryKeyIndex.put(values[idx], newRow);
//				}
				
				while ((newRow = br.readLine()) != null) {
					
					i = 0;
					StringTokenizer st = new StringTokenizer(newRow, DELIM, true);
					values = new String[Main.columnDataTypeMapping.size()];
				    while (st.hasMoreTokens()) {
				    	values[i] = getNext(st);
				    	i++;
				    }

				    //System.out.println("token::" + Arrays.toString(values));
				    Main.primaryKeyIndex.put(Long.parseLong(values[idx]), values);
				    
				    
				}
				
			}else{
				idx1 = Main.columnOrderMapping.get(primaryKeyList.get(0));
				idx2 = Main.columnOrderMapping.get(primaryKeyList.get(1));
				
				
				while ((newRow = br.readLine()) != null) {
					//values = newRow.split("\\|", -1);
					i = 0;
					StringTokenizer st = new StringTokenizer(newRow, DELIM, true);
					values = new String[Main.columnDataTypeMapping.size()];
				    while (st.hasMoreTokens() && i < Main.columnDataTypeMapping.size()) {
				    	
				    	values[i] = getNext(st);
				    	//System.out.println(i + "\t" + values[i]);
				    	i++;
				    }
					keyBuilder = values[idx1];
					Main.primaryKeyIndex.put(Long.parseLong(keyBuilder.concat(values[idx2])), values);
				}
				
			}
			
			
			//System.out.println("pk::" + Main.primaryKeyIndex);
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void sortMyTable(String columnName, List<String> primaryKeyList) throws IOException {
		Map map = new TreeMap<>();
		Long newRow;
		String keyBuilder = "";

		/*
		File file;
		if (System.getProperty("user.home").contains("deepti")) {
			System.out.println("local");
			file = new File(Main.myTableName + ".csv");
		} else {

			file = new File("data/" + Main.myTableName + ".csv");
		}

		BufferedReader br = new BufferedReader(new FileReader(file));
		*/
		
		String values[] = null;
		List<Long> list = null;
		int idx = -1;
		int idpk = -1;
		Iterator<Entry<Long, String[]>> it = Main.primaryKeyIndex.entrySet().iterator();
		//System.out.println("pkl::" + Main.primaryKeyIndex.entrySet());
		
		
		
		idx = Main.columnOrderMapping.get(columnName);
		String ptype = Main.columnDataTypeMapping.get(columnName);
		Main.SQLDataType ptype1 = Main.SQLDataType.valueOf(ptype);
		
		while (it.hasNext()) {
			
			Entry<Long, String[]> e = it.next();
			values = e.getValue();
			newRow = e.getKey();
			

			if (ptype1 == Main.SQLDataType.sqlint) {

				int key = Integer.parseInt(values[idx]);
				if (map.containsKey(key)) {
					list = (List<Long>) map.get(key);
					list.add(newRow);
					map.put(key, list);
				} else {
					list = new ArrayList<Long>();
					list.add(newRow);
					map.put(key, list);
				}

			} else if (ptype1 == Main.SQLDataType.DECIMAL || ptype1 == Main.SQLDataType.decimal) {
				Double key = Double.parseDouble(values[idx]);
				if (map.containsKey(key)) {
					list = (List<Long>) map.get(key);
					list.add(newRow);
					map.put(key, list);
				} else {
					list = new ArrayList<Long>();

					list.add(newRow);
					map.put(key, list);
				}
			} else {
				// (ptype1 == Main.SQLDataType.string)

				String key = values[idx];
				if (map.containsKey(key)) {
					list = (List<Long>) map.get(key);
					list.add(newRow);
					map.put(key, list);
				} else {
					list = new ArrayList<Long>();
					list.add(newRow);
					map.put(key, list);
				}
			}

		}
		Main.columnIndex.put(columnName, map);
		
		//System.out.println("colIdx:" + Main.columnIndex);

	}

	public static List<Long> sortOnIndex2(String columnName, List<Long> PKList) throws IOException {

		List<Long> PKListSorted = new ArrayList<>();

		Map map = new TreeMap<>();
		String values[] = null;
		List<Long> list = null;
		int idx = -1;
		
		for (Long rowString : PKList) {

			values = Main.primaryKeyIndex.get(rowString);
			idx = Main.columnOrderMapping.get(columnName);
			String ptype = Main.columnDataTypeMapping.get(columnName);
			Main.SQLDataType ptype1 = Main.SQLDataType.valueOf(ptype);
			
			if (ptype1 == Main.SQLDataType.sqlint) {
				int key = Integer.parseInt(values[idx]);
				if (map.containsKey(key)) {
					list = (List<Long>) map.get(key);
					list.add(rowString);
					map.put(key, list);
				} else {
					list = new ArrayList<Long>();
					list.add(rowString);
					map.put(key, list);
				}
			} else if (ptype1 == Main.SQLDataType.DECIMAL || ptype1 == Main.SQLDataType.decimal) {
				Double key = Double.parseDouble(values[idx]);
				if (map.containsKey(key)) {
					list = (List<Long>) map.get(key);
					list.add(rowString);
					map.put(key, list);
				} else {
					list = new ArrayList<Long>();

					list.add(rowString);
					map.put(key, list);
				}
			} else {
				String key = values[idx];
				if (map.containsKey(key)) {
					list = (List<Long>) map.get(key);
					list.add(rowString);
					map.put(key, list);
				} else {
					list = new ArrayList<Long>();
					
					
					list.add(rowString);
					map.put(key, list);
				}
			}
		}
		Iterator iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry entry = (Entry) iterator.next();
			for (Long rowS : (ArrayList<Long>) entry.getValue()) {
				PKListSorted.add(rowS);
			}
		}
		return PKListSorted;
	}
}
