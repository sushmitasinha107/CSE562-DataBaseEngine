package dubstep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
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


		if(Main.inmem){
			makePrimaryMapping(Main.primaryKeyList);

            makePrimaryMapping(Main.primaryKeyList);
		for (String indexColumn : Main.primaryKeyList) {
			sortMyTable(indexColumn, Main.primaryKeyList, "create");
		}
		
		for (String indexColumn : indexKeyList) {
			sortMyTable(Main.myTableName + "." + indexColumn, Main.primaryKeyList, "create");
		}
	}
		/*
		
		if (!Main.inmem) {
			if(Main.myTableName.equals("LINEITEM")){
				sortMyTable("LINEITEM.RETURNFLAG" , Main.primaryKeyList, "create");
			
				sortMyTable("LINEITEM.RECEIPTDATE" , Main.primaryKeyList, "create");
				
			}
			
		}*/
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
		if (System.getProperty("user.home").contains("deepti") || System.getProperty("user.home").contains("sushmitasinha")) {
			System.out.println("localq");
			file = new File(Main.myTableName + ".csv");
		} else {

			file = new File("data/" + Main.myTableName + ".csv");
		}

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
				    if(!Main.inmem)
				    	Main.primaryKeyIndexOD.put(Long.parseLong(values[idx]), newRow);
				    
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
					keyBuilder = values[idx1].concat(values[idx2]);
					
					Main.primaryKeyIndex.put(Long.parseLong(keyBuilder), values);
					if(!Main.inmem)
						Main.primaryKeyIndexOD.put(Long.parseLong(keyBuilder), newRow);
				}
				
			}
			
			
			//System.out.println("pk::" + Main.primaryKeyIndex);
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void sortMyTable(String columnName, List<String> primaryKeyList, String queryInstance) throws IOException {
		TreeMap map = new TreeMap<>();
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
		if (Main.inmem) {
			Main.columnIndex.put(columnName, map);
		}
		else{

			FileWriter writer = null;
			if (queryInstance.equals("create")) {
				
				/*
				writer = new FileWriter(columnName + "1.csv");
				Main.columnIndexOnDisk.add(columnName + "1");
				Iterator iterator = map.entrySet().iterator();
				while (iterator.hasNext()) {
					Map.Entry entry = (Entry) iterator.next();
					for (String rowString : (ArrayList<String>) entry.getValue()) {
						writer.write(Main.primaryKeyIndex.get(rowString));
						writer.write('\n');
					}
				}
				
				*/
				//returnflag,linestatus
				
				writer = new FileWriter(columnName + "1.csv");
				Main.columnIndexOnDisk.add(columnName + "1");
				
				Iterator iterator = map.entrySet().iterator();
				
				while (iterator.hasNext()) {
					Map.Entry entry = (Entry) iterator.next();

					// if multiple rows have same index value(clustered)
					List<Long> toOrderByElement2 = (ArrayList<Long>) entry.getValue();

					if (toOrderByElement2.size() > 1) {
						// 2 order by column criteria, then sort the cluster based
						// on second column
						if(columnName.equals("LINEITEM.RETURNFLAG")){
							toOrderByElement2 = MyCreateTable.sortOnIndex2("LINEITEM.LINESTATUS", toOrderByElement2);
						}
						else if(columnName.equals("LINEITEM.RECEIPTDATE")){
							toOrderByElement2 = MyCreateTable.sortOnIndex2("LINEITEM.PARTKEY", toOrderByElement2);
						}
						
						
					}
					
					
					//System.out.println("toOrderByElement2"+ toOrderByElement2);
						//System.out.println(Main.primaryKeyIndexOD);
					
						// clustered index
						for (Long rowString : toOrderByElement2) {
							// read new row from (PK,entire row ) map
							//newRow = Main.primaryKeyIndex.get(rowString);
							//String joined = String.join(",", name);
							writer.write(Main.primaryKeyIndexOD.get(rowString));
							writer.write('\n');
					}

				}
				
			} else {
				if (Main.orderByElementsSortOrder.get(columnName) == 1) {
					writer = new FileWriter(columnName + "1.csv");
					Main.columnIndexOnDisk.add(columnName + "1");
					Iterator iterator = map.entrySet().iterator();
					while (iterator.hasNext()) {
						Map.Entry entry = (Entry) iterator.next();
						for (Long rowString : (ArrayList<Long>) entry.getValue()) {
							
							writer.write(Main.primaryKeyIndexOD.get(rowString));
							
							writer.write('\n');
						}
					}

				} else {
					writer = new FileWriter(columnName + "2.csv");
					Main.columnIndexOnDisk.add(columnName + "2");
					Iterator iterator = map.descendingMap().entrySet().iterator();
					while (iterator.hasNext()) {
						Map.Entry entry = (Entry) iterator.next();
						for (Long rowString : (ArrayList<Long>) entry.getValue()) {
							writer.write(Main.primaryKeyIndexOD.get(rowString));
							writer.write('\n');
						}
					}
				}
			}
			map.clear();
			writer.close();
		
		}
		
		
		
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
