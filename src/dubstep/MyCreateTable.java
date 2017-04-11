import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class MyCreateTable {

	public void createTable() throws IOException {
		Main.table = (CreateTable) Main.query;
		Main.myTableName = Main.table.getTable().getName();
		Main.tableData = new TableData();
		Main.columnNames = Main.table.getColumnDefinitions();
		int i = 0;
		for (ColumnDefinition col : Main.columnNames) {
			Main.columnOrderMapping.put(col.getColumnName(), i);

			String dtype = col.getColDataType().getDataType();
			if (dtype.equals("int")) {
				dtype = "sqlint";
			} else if (dtype.equals("char")) {
				dtype = "sqlchar";
			}
			Main.columnDataTypeMapping.put(col.getColumnName(), dtype);
			i++;
		}
		Main.tableData.setColumnDataTypeMapping(Main.columnDataTypeMapping);
		Main.tableData.setColumnOrderMapping(Main.columnOrderMapping);

		Main.tableMapping.put(Main.myTableName, Main.tableData);
		for (ColumnDefinition col : Main.columnNames) {
			sortMyTable(col.getColumnName());
		}

	}

	@SuppressWarnings("unchecked")
	public static void sortMyTable(String columnName) throws IOException {
		Map map = new TreeMap<>();
		String newRow = "";

		File file = new File(Main.myTableName + ".csv");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String values[] = null;
		List<String> list = null;
		try {
			while ((newRow = br.readLine()) != null) {
				// System.out.println(newRow);
				values = newRow.split("\\|", -1);
				int idx = Main.columnOrderMapping.get(columnName);
				
				if (map.containsKey(values[idx])) {
					list = (List<String>) map.get(values[idx]);
					list.add(newRow);
					map.put(values[idx],list);
				} else {
					list = new ArrayList<String>();
					list.add(newRow);
					map.put(values[idx],list);
				}
			}
			Main.columnIndex.put(columnName, map);
			//System.out.println(map);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}
}
