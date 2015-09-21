import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Tester {

	public static void main(String[] args) throws Exception {
		/*int input[] = { 0, 1, 2, 1, 2, 2, 3, 41, 9, 39, 28, 1, 30, 38, 39, 31,
				-1, 42, 28 };
		RandomAccessFile f = null;
		List<String> lines = new ArrayList<String>();
		try {
			f = new RandomAccessFile("countries.csv", "r");
			String s = new String();
			// int counter = 0;
			while ((s = f.readLine()) != null) {
				// System.out.println("Records :" + s + " and Record Number : "
				// + counter++);
				lines.add(s);
			}
		} finally {
			try {
				if (f != null) {
					f.close();
				}
			} catch (Exception e) {
				System.out.println("");
			}
		}*/

		DBSystem dbs = new DBSystem();
		dbs.readConfig("config.txt");
		dbs.populateDBInfo();

		/*for (int inp : input) {
			if (inp != -1) {
				String op = dbs.getRecord("countries", inp);
				if (lines.get(inp).equals(op)) {

				} else {
					System.out.println();
					System.out.println("Fail for record number " + inp
							+ " expected = '" + lines.get(inp) + "' actual= '"
							+ op + "'");
					System.exit(-1);
				}
			} else
				dbs.insertRecord("countries", "record");
		}*/
		//dbs.insertRecord("countries", "\"302613\",\"ZZ\",\"Unknown or unassigned country\",\"AF\""+"\n");
		//dbs.insertRecord("countries", "\"302613\",\"ZZ\",\"Unknown or unassigned country\",\"AF\""+"\n");
		//dbs.insertRecord("countries", "\"302613\",\"ZZ\",\"Unknown or unassigned country\",\"AF\""+"\n");
		
		//dbs.insertRecord("airports","\"777777\",\"small_airport\",\"Glorioso Islands Airstrip\",\"-12\",\"47\",\"TF\",\"TF-U-A\""+ "\n");
		//dbs.insertRecord("airports","\"666666\",\"small_airport\",\"Glorioso Islands Airstrip\",\"-12\",\"47\",\"TF\",\"TF-U-A\""+ "\n");
		
		String[] query = {
				//"SELECT department.CONTINENT, countries.CONTINENT, countries.NAME FROM countries INNER JOIN department ON department.CONTINENT=countries.CONTINENT"
				"SELECT airports.ID, countries.ID, countries.NAME FROM airports INNER JOIN countries ON airports.ID=countries.ID INNER JOIN department ON department.ID=countries.ID"
				//"Select ID,NAME from airports order by NAME"
				};
		
		/*String[][] tables = {{"countries","department","airports"},{"ID","CODE","NAME","CONTINENT"},{"ID","CODE","NAME","CONTINENT"},{"ID","TYPE","NAME","LATITUDE","LONGITUDE","COUNTRY","REGION"}};
		
		//tables[0][0]
				
		for(int a = 0; a < tables[0].length; a++){
			for(int b = 0; b < tables[a+1].length; b++)
				//dbs.V(tables[0][a], tables[a+1][b]);
				System.out.println("Table : " + tables[0][a] + " Column : " + tables[a+1][b] + " Distinct values : " + dbs.V(tables[0][a], tables[a+1][b]));
				//System.out.println("Table : " + tables[0][a] + " Column : " + tables[a+1][b]);
		}*/
		
/*	"Select NAME,LATITUDE,LONGITUDE from airports where LATITUDE>=40 and LATITUDE<=42 order by LATITUDE"
					
 * "select code, ID from countries where code=\"USEF\" or name=\"Andorra\"",
		"select code, ID from countries where code=\"5\"",
		"select distinct ID from countries where code=\"5\"",
		"select * from countries where name = \"India\" group by (id) having (id) > 10 order by id",
		"create table student (id number, name varchar2(20))",
		"select col1 from notexisting",
		"Select * from countries",
		"Select TYPE,NAME from airports",
		"Select ID from airports where TYPE='small_airport'",
		"Select ID from airports where TYPE like 'SMALL_Airport'",
		"Select ID,NAME,CODE from countries where CONTINENT='OC' or NAME='India'", 
		"Select ID,NAME from airports order by NAME",
		"Select NAME,COUNTRY from airports where LATITUDE>=40 and LATITUDE<=42 order by NAME"};*/

		/*for(int j=1; j<= query.length; j++){
			System.out.print("\nResult for Query " + j  + ": ");
			System.out.println("\n_________________________________");
			dbs.queryType(query[j-1]);
		}*/
		
		//dbs.queryType(query[6]);
		//dbs.queryType(query[1]);
		//dbs.queryType(query[2]);
		//dbs.queryType(query[3]);
		dbs.queryType(query[0]);

		//dbs.queryType(query[0]);
		//dbs.queryType(query[1]);
		System.exit(0);
	}

}
