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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class MyCreateTable {
	public static Map<Long, String[]> primaryKeyIndexCr = new HashMap<Long, String[]>();
	public static TableData tableDataCr ;
	public static TableData tableDataJoin ;

	public static Index primaryKeyCr;
	public static Index indexKeyCr;
	public static List<String> indexKeyListCr;
	public static final String DELIM = "\\|";
	public static String myTableNameCr = "";
	public static List<ColumnDefinition> columnNamesCr = null;
	
	public static List<String> primaryKeyListCr = new ArrayList<>();
	public static Map<String, Integer> columnOrderMappingCr = new HashMap<String, Integer>();
	public static Map<String, String> columnDataTypeMappingCr = new HashMap<String, String>();

	public static void createTable(Statement query) throws IOException {

		CreateTable table = (CreateTable) query;
		List<Index> tableIndex = table.getIndexes();
		List<String> indexKeyList = new ArrayList<>();
		primaryKeyListCr = new ArrayList<>();
		primaryKeyIndexCr = new HashMap<Long, String[]>();
		myTableNameCr = table.getTable().getName();
		tableDataCr = new TableData();
		tableDataJoin = new TableData();
		columnNamesCr = table.getColumnDefinitions();

		int i = 0;
		for (ColumnDefinition col : columnNamesCr) {
			columnOrderMappingCr.put(myTableNameCr + "." + col.getColumnName(), i);
			//columnOrderMappingCr.put(col.getColumnName(), i);

			
			Main.columnOrderMapping.put(myTableNameCr + "." + col.getColumnName(), i);
			//Main.columnOrderMapping.put(col.getColumnName(), i);

			String dtype = col.getColDataType().getDataType();
			if (dtype.equalsIgnoreCase("INT")) {
				dtype = "sqlint";
			} else if (dtype.equalsIgnoreCase("CHAR")) {
				dtype = "sqlchar";
			}

			columnDataTypeMappingCr.put(myTableNameCr + "." + col.getColumnName(), dtype);
			//columnDataTypeMappingCr.put(col.getColumnName(), dtype);

			Main.columnDataTypeMapping.put(myTableNameCr + "." + col.getColumnName(), dtype);
			//Main.columnDataTypeMapping.put(col.getColumnName(), dtype);

			
			i++;
		}
				//for tablemappingjoin
		tableDataJoin.setColumnDataTypeMapping(Main.columnDataTypeMapping);
		tableDataJoin.setColumnOrderMapping(Main.columnOrderMapping);
		tableDataJoin.setPrimaryKeyList(Main.primaryKeyList);

		

		if (tableIndex != null) {
			for (Index indxValue : tableIndex) {
				if (indxValue.getType().length() == 5) {

					indexKeyList.addAll(indxValue.getColumnsNames());
				}

				else {
					for (String pkCol : indxValue.getColumnsNames()) {

						String pk = myTableNameCr + "." + pkCol;
						primaryKeyListCr.add(pk);

					}
				}
			}
		}


		//if (Main.inmem) {
			
			makePrimaryMapping(primaryKeyListCr);

			for (String indexColumn : primaryKeyListCr) {
				sortMyTable(indexColumn, primaryKeyListCr, false);
			}

			for (String indexColumn : indexKeyList) {
				sortMyTable(myTableNameCr + "." + indexColumn, primaryKeyListCr, false);
				//sortMyTable(indexColumn, primaryKeyListCr);
			}
			
			
			if(myTableNameCr.equals("LINEITEM")){
				//sortMyTable("LINEITEM.RETURNFLAG" , primaryKeyListCr, false);
				//sortMyTable("LINEITEM.LINESTATUS" , primaryKeyListCr, false);
			}
		//}
		/*else{
			
			// onDisk sort
			//pass index of column to sort
			//get index of "LINEITEM.RETURNFLAG", "LINEITEM.RECEIPTDATE" and "LINEITEM.LINESTATUS"
			
			int oIdx = Main.columnOrderMapping.get("LINEITEM.RETURNFLAG");
			//System.out.println("oIdx::" + oIdx);
			
			ExternalSort.onDiskSort(oIdx, "LINEITEM.RETURNFLAG");
			
			oIdx = Main.columnOrderMapping.get("LINEITEM.RECEIPTDATE");
			//System.out.println("oIdx::" + oIdx);
			
			ExternalSort.onDiskSort(oIdx, "LINEITEM.RECEIPTDATE");
			
			oIdx = Main.columnOrderMapping.get("LINEITEM.QUANTITY");
			//System.out.println("oIdx::" + oIdx);
			
			ExternalSort.onDiskSort(oIdx, "LINEITEM.QUANTITY");
<<<<<<< HEAD
		}*/
		
		
		
		Main.tableMappingJoin.put(Main.myTableName, tableDataJoin);
		
		
		//}
		
		tableDataCr.setColumnDataTypeMapping(columnDataTypeMappingCr);
		tableDataCr.setColumnOrderMapping(columnOrderMappingCr);
		
		
		tableDataCr.setTableName(myTableNameCr);
		tableDataCr.setPrimaryKeyList(primaryKeyListCr);
		
		
		
		Main.tableMapping.put(myTableNameCr, tableDataCr);
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
		


		//System.out.println("primaryKeyList ::" + primaryKeyList);
		File file;
		file = new File("data/" + myTableNameCr + ".csv");
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
				    primaryKeyIndexCr.put(Long.parseLong(values[idx]), values);
				    
				    
				}
				
			}else{
				
				
				
				idx1 = columnOrderMappingCr.get(primaryKeyList.get(0));
				idx2 = columnOrderMappingCr.get(primaryKeyList.get(1));
				
				
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
					primaryKeyIndexCr.put(Long.parseLong(keyBuilder.concat(values[idx2])), values);
				}
				
			}
			tableDataJoin.setPrimaryKeyIndex(primaryKeyIndexCr);
			//System.out.println("pk::" + Main.primaryKeyIndex);
									
			//System.out.println("pk::" + Main.primaryKeyIndex);
			tableDataCr.setPrimaryKeyIndex(primaryKeyIndexCr);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void sortMyTable(String columnName, List<String> primaryKeyList, Boolean fly) throws IOException {
		columnName = columnName.trim();
		Map map = new TreeMap<>();
		Long newRow;
		String keyBuilder = "";
		
		String values[] = null;
		List<Long> list = null;
		int idx = -1;
		int idpk = -1;
		if(fly == true){
			String strName = columnName.split("\\.")[0];
			TableData temp = new TableData();
			temp = Main.tableMapping.get(strName.trim());
			if(temp != null){
			primaryKeyIndexCr = temp.getPrimaryKeyIndex();
			}else{
				primaryKeyIndexCr = Main.primaryKeyIndex;
			}
		}
		Iterator<Entry<Long, String[]>> it = primaryKeyIndexCr.entrySet().iterator();
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
	}

	public static List<Long> sortOnIndex2(String columnName, List<Long> PKList) throws IOException {

		List<Long> PKListSorted = new ArrayList<>();

		Map map = new TreeMap<>();
		String values[] = null;
		List<Long> list = null;
		int idx = -1;
		
		for (Long rowString : PKList) {
			
			primaryKeyIndexCr = new HashMap<Long, String[]>();
			TableData td = new TableData();
			
			td = Main.tableMapping.get(Main.myTableName);

			//td.setTableName(Main.myTableName);
			if(!columnName.contains("."))
			columnName = Main.myTableName +"."+ columnName;
			 
			 //System.out.println(columnName);

			primaryKeyIndexCr = new HashMap<Long, String[]>();
			
						
			if(!columnName.contains(".")){
				
				columnName =  Main.myTableName +"."+ columnName;
			
			}
			 
			 //System.out.println(columnName);
			primaryKeyIndexCr = td.getPrimaryKeyIndex();
			values = primaryKeyIndexCr.get(rowString);

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
