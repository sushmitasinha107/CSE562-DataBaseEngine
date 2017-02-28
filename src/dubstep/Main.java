package dubstep;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Table;

public class Main {

	public static void main(String[] args) throws ParseException {

		Scanner sc = new Scanner(System.in);
		String tableStructure = sc.nextLine();
		StringReader tableInput = new StringReader(tableStructure);
		CCJSqlParser tableParser = new CCJSqlParser(tableInput);
		Statement tableStatement = tableParser.Statement();
		CreateTable table = (CreateTable) tableStatement ;
		String myTableName  =  table.getTable().getName();
		System.out.println("Try TableName"+": "+ myTableName );
		List<ColumnDefinition> columnNames = table.getColumnDefinitions();
		System.out.println("Try Col"+": "+ columnNames );
		Map<String,Integer> columnOrderMapping = new HashMap<String,Integer>();
		int i = 0;
		for(ColumnDefinition col : columnNames){
			System.out.println("Try Col name"+": "+ col.getColumnName() );
			System.out.println("Try Col datatype"+": "+ col.getColDataType() );
			columnOrderMapping.put(col.getColumnName() , i);
			i++;
		}
		System.out.println(columnOrderMapping);
		while (sc.hasNext()) {
			String inputString = sc.nextLine();
			StringReader input = new StringReader(inputString);
			CCJSqlParser parser = new CCJSqlParser(input);
			try {
				Statement query = parser.Statement();
				if (query instanceof Select) {
					Select select = (Select) query;
					PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
					List<SelectItem> selectItems = plainSelect.getSelectItems();
					List<Integer> selectIndexes = getSelectIndexesfromMap(selectItems,columnOrderMapping);
					//String tableName = plainSelect.getFromItem().toString();
					 System.out.println("selectItems: " + selectIndexes);
					readFromFile(myTableName, selectIndexes);

				} else {
					 //System.out.println("Not of type select");
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

	}

	private static List<Integer> getSelectIndexesfromMap(List<SelectItem> items, Map<String, Integer> map) {
		// TODO Auto-generated method stub
		System.out.println("I am in getSelectIndexesfromMap"+ map);
		List<Integer> index = new ArrayList<Integer>();
		
		
		for(int i =0; i< items.size(); i++){
			int num = map.get(items.get(i).toString());
			index.add(num);
		}
		return index;	
	}

	public static void readFromFile(String tableName, List<Integer> index) {
		//File file = new File("data/" + tableName + ".csv");
		File file = new File( tableName + ".csv");
		try {
			Scanner sc = new Scanner(file);
			while (sc.hasNext()) {
				
				String newRow = sc.nextLine();
				StringBuilder sbuilder = new StringBuilder();
				String[] values = newRow.split(",");
				for(int i=0 ; i<index.size(); i++){
					sbuilder.append(values[index.get(i)]);
					if(i!=index.size()-1)
						sbuilder.append("|");
				}
				System.out.println("line"+sbuilder.toString());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

}
