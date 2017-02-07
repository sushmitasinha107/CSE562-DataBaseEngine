package dubstep;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.util.Scanner;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Table;


public class Main {

	public static void main(String[] args) {

		Scanner sc = new Scanner(System.in);
		
		String inputString = sc.nextLine();
		StringReader input = new StringReader(inputString);
		
		//StringReader input = new StringReader("select * from tableE");

		
        CCJSqlParser parser = new CCJSqlParser(input);
        
        try {
			Statement query = parser.Statement();
			
			if(query instanceof Select){
			    			
				System.out.println("Select statement");
				
				Select select = (Select) query;
				PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
				String tableName = plainSelect.getFromItem().toString();
				System.out.println("table: " + tableName);
				
				readFromFile(tableName);

			  } else {
				  System.out.println("Not of type select");
			  }
			
		} catch (ParseException e) {
			e.printStackTrace();
		}


	}
	
	public static void readFromFile(String tableName){
		
		File file = new File(tableName + ".csv");
		try {
			Scanner sc = new Scanner(file);
			while(sc.hasNext()){
				System.out.println(sc.nextLine());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

}
