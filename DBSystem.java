import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.EJoinType;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.TStatementList;
import gudusoft.gsqlparser.nodes.TColumnDefinition;
import gudusoft.gsqlparser.nodes.TColumnDefinitionList;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TGroupBy;
import gudusoft.gsqlparser.nodes.TJoin;
import gudusoft.gsqlparser.nodes.TJoinItem;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBSystem {
	public static String selectTable = null;
	public static int operationSelected = 0;
	public static final int READ_OPERATION = 1;
	public static final int WRITE_OPERATION = 2;
	public static HashMap<String, TableDetails> allTables = new HashMap<String, TableDetails>();
	private static LRUCache cache = null;

	public static int _pageSize;
	public static int _max_page_to_cache;

	public static String _select_TargetTable = null;
	public static String _select_Columns = null;
	public static String _select_Condition = null;
	public static int[] _select_Column_order = null;
	List<Integer> select_Column_Order_List = null;
	List select_Column_Order_Table_List = null;
	public static List _select_OrderBy_Column = null;
	public static boolean _has_Where_Cond = false;
	public static List _select_where_Column = null;
	public static List _select_where_Column_cond = null;
	public static List _select_where_Column_Value = null;
	public static List _select_where_Multi_Connector = null;
	TGSqlParser sqlparser = null;
	private boolean _has_Join;
	private static List _join_Left_Cond = null;
	private static List _join_Right_Cond = null;
	private static List _join_Type = null;
	//private List _select_targetTable_List.add(selectStmt.tables.getTable(i).toString());;
	private List _select_targetTable_List = null;
	public static int orderByQueryCount = 0; 

	// public static final String ATTR_NAME = "name";
	// public static final String ATTR_TYPE = "type";

	// public String _value;
	// public String _tableName;
	// public Map<String,String> _columns;
	public static Map<String, List<String>> _tableList = new HashMap<String, List<String>>();
	// public static Map<String,String> _config = new HashMap<String,String>();
	// public static Map<Integer,RecordDetails> _recordDetail = new
	// HashMap<Integer,RecordDetails>();
	public static String path;
	
	public static Map<String, List<String>> _colTypeList = new HashMap<String, List<String>>();

	// public static final String RECORD_DETAILS = "recordDetails";
	// public static final String PAGE_DETAILS = "pageDetails";

	public void readConfig(String configFilePath) {
		String pair[], line;
		boolean readTable = false;
		boolean readColumn = true;

		try {
			RandomAccessFile br = new RandomAccessFile(configFilePath, "r");
			line = br.readLine();
			StringBuffer tableName = new StringBuffer(10);
			List colList, colTypeList;
			while (line != null) {
				pair = line.split(" ");
				if (line.contains("PAGE_SIZE")) {
					_pageSize = Integer.parseInt(pair[1]);
				} else if (line.contains("NUM_PAGES")) {
					_max_page_to_cache = Integer.parseInt(pair[1]);
				} else if (line.contains("PATH_FOR_DATA")) {
					path = pair[1];
				} else if (line.toLowerCase().contains("begin")) {
					tableName.setLength(0);
					readTable = true;
				} else if (readTable) {
					tableName.append(line.toLowerCase());
					_tableList.put(tableName.toString(), null);
					readTable = false;
					readColumn = true;
				} else if (readColumn) {
					if (line.toLowerCase().contains("primary_key")) {

					} else if (line.toLowerCase().contains("end")) {
						readColumn = false;
					} else {
						colList = _tableList.get(tableName.toString());
						if (colList == null) {
							colList = new ArrayList<String>(10);
						}
						
						colTypeList = _colTypeList.get(tableName.toString());
						if (colTypeList == null) {
							colTypeList = new ArrayList<String>(10);
						}

						colList.add(line.split(", ")[0]);
						colTypeList.add(line.split(", ")[1]);
						
						_tableList.put(tableName.toString(), colList);
						_colTypeList.put(tableName.toString(), colTypeList);
					}
				}
				line = br.readLine();
			}

		} catch (Exception e) {
			System.out.println("Problem reading congif");
		}

	}

	public void populateDBInfo() {
		for (Map.Entry<String, List<String>> entry : _tableList.entrySet()) {
			allTables.put(entry.getKey(), ReadCSV.readCSV(entry.getKey()));
		}
	}

	public String getRecord(String tableName, int record) {
		if (cache == null) {
			cache = new LRUCache(_max_page_to_cache);
		}
		TableDetails dual = DBSystem.allTables.get(tableName);
		HashMap recordDetail = (HashMap) dual.getRecordDetails();
		HashMap pageDetail = (HashMap) dual.getPageDetails();
		RecordDetails rd = (RecordDetails) recordDetail.get(record);
		int pageNumber = rd.getPageNumber();
		String page = null;
		if (cache.get(tableName + pageNumber) != null) {
			// System.out.println("HIT :" + (tableName + pageNumber));
			page = cache.get(tableName + pageNumber);
		} else {
			// System.out.println("MISS :" + (tableName + pageNumber));
			PageDetails pgd = (PageDetails) pageDetail.get(pageNumber);
			byte[] buf = new byte[_pageSize];
			try {
				RandomAccessFile raf = new RandomAccessFile(tableName + ".csv","r");
				raf.seek(pgd.getStartIndex());
				raf.read(buf, 0, (int) (pgd.getEndIndex() - pgd.getStartIndex()));
				page = new String(buf);
				cache.put(tableName + pageNumber, page);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (page != null) {
			// page = page.trim();
			String[] records = page.split("\n");
			// System.out.println(records[rd.getRecordNumberInPage() - 1]);
			// if(record > 8)
			// return records[rd.getRecordNumberInPage()-2];
			return records[rd.getRecordNumberInPage() - 1].trim();
			// return records[rd.getRecordNumberInPage()];
		}
		return null;
	}

	public void insertRecord(String tableName, String data) {
		// System.out.println("Please enter the row : ");
		TableDetails dual = DBSystem.allTables.get(tableName);
		List colList = _tableList.get(tableName);
		String[] dataSplit = data.split(",");
		if(dataSplit.length != colList.size())
			System.out.println("PROBLEM with the new record");
		File file = new File(tableName + ".csv");
		BufferedWriter bufferWritter = null;
		try {
			FileWriter fileWritter = new FileWriter(file.getName(), true);
			bufferWritter = new BufferedWriter(fileWritter);
			bufferWritter.write(data);
			bufferWritter.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (bufferWritter != null)
					bufferWritter.close();
			} catch (Exception e) {

			}
		}
		 
		PageDetails p = ((PageDetails)(dual.getPageDetails().get(dual.getLastPageOfTable())));
		int lastPage = dual.getLastPageOfTable();
		int noOfRecods = dual.getNumberOfRecords();
		HashMap recordDetailHash = dual.getRecordDetails();
		RecordDetails rd = null;
		if((p.getEndIndex() - p.getStartIndex()) + data.length() <= _pageSize){
			//same page
			 rd = new RecordDetails(lastPage, p.getNumberOfRecoredsInPage() + 1);
		} else {
			// create a new page
			 rd = new RecordDetails(lastPage + 1, 1);
			 dual.setLastPageOfTable(lastPage + 1);
			 HashMap pd = dual.getPageDetails();
			 pd.put(lastPage + 1, new PageDetails(p.getEndIndex() + 1,(p.getEndIndex() + 1) + data.length(), 1));
			 dual.setPageDetails(pd);
		}
		
		recordDetailHash.put(noOfRecods + 1, rd);
		dual.setRecordDetails(recordDetailHash);
		dual.setNumberOfRecords(noOfRecods + 1);
		
		for(int a=0; a < dataSplit.length; a++){
			dataSplit[a] = dataSplit[a].trim().toLowerCase();
			if(dataSplit[a].startsWith("\"") && dataSplit[a].endsWith("\"")){
				dataSplit[a] = dataSplit[a].substring(1,dataSplit[a].length()-1);
			}
		}
		
		HashMap index = dual.getIndex();
		for(int c = 0; c < dataSplit.length; c++){
			System.out.println("C" + c + " size :" + ((HashMap)index.get(c)).size());
			HashMap map = (HashMap)index.get(c);
			if(map == null)
				continue;
			if(map.containsKey(dataSplit[c])){
				List list = (List)map.get(dataSplit[c]);
				list.add(noOfRecods + 1);
			}else{
				List list = new ArrayList<>();
				list.add(noOfRecods + 1);
				map.put(dataSplit[c], list);
			}
		}
		
		//dual.setIndex(index);
		HashMap indexAftr = dual.getIndex();
		for(int d= 0; d < indexAftr.size(); d++){
			System.out.println(((HashMap)indexAftr.get(d)).size());
		}
		//System.out.println(getRecord(tableName, noOfRecods + 1));
	}

	public void queryType(String query) {
		// Check syntax of query
		if (sqlparser == null)
			sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		sqlparser.sqltext = query;

		int ret = sqlparser.parse();

		if (ret == 0) {
			// syntax ok! Now analyze the sql statement
			TStatementList sqlstatements = sqlparser.sqlstatements;

			for (int i = 0; i < sqlstatements.size(); i++) {
				analyzeStmt(sqlstatements.get(i));
				System.out.println("");
			}
		} else {
			System.out.println("Query syntax not OK !!! \n"
					+ sqlparser.getErrormessage());
		}

	}

	private void analyzeStmt(TCustomSqlStatement stmt) {

		switch (stmt.sqlstatementtype) {
		case sstselect:
			selectCommand((TSelectSqlStatement) stmt);
			break;
		case sstcreatetable:
			createCommand((TCreateTableSqlStatement) stmt);
			break;
		case sstupdate:
			break;

		case sstaltertable:
			break;
		case sstcreateview:
			break;
		default:
			System.out.println(stmt.sqlstatementtype.toString());
		}
	}

	/**
	 * 
	 * @param query
	 */
	private void createCommand(TCreateTableSqlStatement createStmt) {
		String tableName = createStmt.getTableName().toString();
		if (_tableList.containsKey(tableName.toLowerCase())) {
			System.out.println("Table already exists!!");
			// System.exit(0);
		}

		System.out.print("\nQuery type: \t Create");

		System.out.printf("\nTablename: \t%s", tableName);

		// create two new file (i.e. ".csv" and ".data" files)
		File csv = new File(tableName + ".csv");

		File data = new File(tableName + ".data");

		// make an entry in the config file
		File file = new File("config.txt");
		BufferedWriter bufferWritter = null;
		try {
			FileWriter fw = new FileWriter(csv.getName(), true);
			BufferedWriter buffWrt = new BufferedWriter(fw);
			buffWrt.write("");

			FileWriter fileWritter = new FileWriter(file.getName(), true);
			bufferWritter = new BufferedWriter(fileWritter);

			FileWriter dataWriter = new FileWriter(data.getName(), true);
			BufferedWriter bufferWritter1 = new BufferedWriter(dataWriter);

			bufferWritter.write("\nBEGIN");
			bufferWritter.write("\n" + tableName);

			System.out.print("\nAttributes: ");

			TColumnDefinitionList columnList = createStmt.getColumnList();

			for (int i = 0; i < columnList.size(); i++) {
				if (i >= 1) {
					System.out.print(",");
				}

				TColumnDefinition cd = columnList.getColumn(i);
				String colName = cd.getColumnName().toString();
				System.out.printf("  %s", colName);

				String colType = cd.getDatatype().toString();
				System.out.printf("  %s", colType);

				bufferWritter.write("\n" + colName + "," + colType);

				bufferWritter1.write("\n" + colName + "," + colType);

			}

			bufferWritter.write("\nEND");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (bufferWritter != null)
					bufferWritter.close();
			} catch (Exception e) {

			}
		}

	}

	private void selectCommand(TSelectSqlStatement selectStmt) {
		_select_TargetTable = null;
		_select_Columns = null;
		_select_Condition = null;
		_select_Column_order = null;
		//_select_Column_Order_List = null;
		select_Column_Order_List = null;
		_has_Where_Cond = false;
		_select_where_Column = null;
		_select_where_Column_cond = null;
		_select_where_Column_Value = null;
		_select_where_Multi_Connector = null;

		String targetTable = "";// "employee";
		//getTable(1)joins.getJoin(0).getTable().toString();
		//if(selectStmt.tables.size() > 0)
			

		for (int i = 0; i < selectStmt.tables.size(); i++ ){
			if(i > 0){
				_has_Join = true;
			}
			if(_select_targetTable_List == null)
				_select_targetTable_List = new ArrayList<>();
			_select_targetTable_List.add(selectStmt.tables.getTable(i).toString());
			targetTable = targetTable + ((i>0) ? "," + selectStmt.tables.getTable(i).toString() : selectStmt.tables.getTable(i).toString());
		}
		
		for(int x=0; x < _select_targetTable_List.size(); x++){
			if (!_tableList.containsKey(((String)_select_targetTable_List.get(x)).toLowerCase())) {
				System.out.println("Query is invalid !!");
				// System.exit(0);
				return;
			}
		}

		System.out.print("\nQuery type: \tSelect");

		_select_TargetTable = targetTable;
		// table name
		System.out.printf("\nTable:   \t%s", targetTable);

		// select list
		TResultColumnList resultColumnList = selectStmt.getResultColumnList();
		System.out.print("\nColumns: ");
		
		select_Column_Order_List = new ArrayList<Integer>();
		select_Column_Order_Table_List = new ArrayList<>();
		
		for (int i = 0; i < resultColumnList.size(); i++) {
			TResultColumn resultColumn = resultColumnList.getResultColumn(i);
			if ("*".equals(resultColumn.getExpr().toString())) {
				for(int x = 0; x < _select_targetTable_List.size(); x++) {
					_select_Columns = _tableList.get(_select_targetTable_List.get(x)).toString();
					String[] cols = _select_Columns.split(",");
					//_select_Column_order = new int[cols.length];
					for (int j = 0; j < cols.length; j++) {
						select_Column_Order_List.add(j);
						select_Column_Order_Table_List.add(_select_targetTable_List.get(x));
						//_select_Column_order[j] = j;
					}
					System.out.printf("\t%s", _tableList.get(_select_targetTable_List.get(x)).toString().substring(1, _select_Columns.length()-1));
				}	
				//_select_Column_order = (int[])select_Column_Order_List.toArray();
				
			} else {
				_select_Columns = resultColumn.getExpr().toString();
				String[] table_col;
				if(_has_Join){
					table_col = _select_Columns.split("\\.");
				
				List tableCols = _tableList.get(table_col[0]);
				for (int b = 0; b < tableCols.size(); b++) {
					if (((String) tableCols.get(b)).equalsIgnoreCase(table_col[1])) {
						//_select_Column_order[i] = b;
						select_Column_Order_List.add(b);
						select_Column_Order_Table_List.add(table_col[0]);
						break;
					}
				}
				//if (_select_Column_order == null)
					//_select_Column_order = new int[resultColumnList.size()];
				// String[] selectCols = _select_Columns.split(",");
				} else {
					List tableCols = _tableList.get(targetTable);
				
				// _select_Column_order = new int[selectCols.length];
				// int index = 0;
				// for(int a =0; a < selectCols.length ; a++){
				//for(int x = 0; x < _select_targetTable_List.size(); x++) {
					
					for (int b = 0; b < tableCols.size(); b++) {
						if (((String) tableCols.get(b)).equalsIgnoreCase(_select_Columns)) {
							select_Column_Order_List.add(b);
							select_Column_Order_Table_List.add(targetTable);
							
							//_select_Column_order[i] = b;
							break;
						}
					}
				}
				// }
				System.out.printf("\t%s", resultColumn.getExpr().toString());
			}
		}

		// distinct clause
		if (selectStmt.getSelectDistinct() != null) {
			long colNumber = selectStmt.getSelectDistinct().getColumnNo();

			// TResultColumn resultColumn = resultColumnList.getResultColumn(i);
			// System.out.println("\nDistinct: " + colNumber);
			System.out.println("\nDistinct: " + resultColumnList);
			// System.out.printf("\nDistinct: \t%s",
			// resultColumnList.getResultColumn((int)colNumber).getExpr().toString()
			// );
		} else {
			System.out.printf("\nDistinct: \t%s", "NA");
		}

		// where clause
		if (selectStmt.getWhereClause() != null) {
			_has_Where_Cond = true;
			getWhereConditions(selectStmt);
			System.out.printf("\nCondition: \t%s", selectStmt.getWhereClause()
					.getCondition().toString());
		}

		else {
			System.out.printf("\nCondition: \t%s", "NA");
		}

		// order by
		if (selectStmt.getOrderbyClause() != null) {
			System.out.printf("\nOrderby:");
			List tableCols = _tableList.get(_select_TargetTable);
			for (int i = 0; i < selectStmt.getOrderbyClause().getItems().size(); i++) {
				System.out.printf("\t%s", selectStmt.getOrderbyClause()
						.getItems().getOrderByItem(i).toString());
				for (int b = 0; b < tableCols.size(); b++) {
					if (((String) tableCols.get(b))
							.equalsIgnoreCase(selectStmt.getOrderbyClause()
									.getItems().getOrderByItem(i).toString())) {
						//if (_select_OrderBy_Column == null)
							_select_OrderBy_Column = new ArrayList();
						_select_OrderBy_Column.add(b);
					}
				}
			}
			
		} else {
			System.out.printf("\nOrderby: \t%s", "NA");
		}

		// group by
		TGroupBy groupBy = selectStmt.getGroupByClause();

		if (groupBy != null) {
			// Assuming only one column in group By clause
			System.out.printf("\nGroupby: \t%s", groupBy.getItems()
					.getGroupByItem(0).getExpr().toString());
		}

		else {
			System.out.printf("\nGroupby: \t%s", "NA");
		}

		// having
		if (groupBy != null && groupBy.getHavingClause() != null) {
			System.out.printf("\nHaving : \t%s", groupBy.getHavingClause());
		}

		else {
			System.out.printf("\nHaving: \t%s", "NA\n");
		}
		
		// Join
		if(_has_Join){
		/*	for(int x = 0; x < selectStmt.joins.size(); x++){*/
			TJoin join = selectStmt.joins.getJoin(0);
			int x = 0;
			for(; x < join.getJoinItems().size(); x++){
				TJoinItem joinItem = join.getJoinItems().getJoinItem(x);
		  
			    //Assuming all will be INNER JOIN
				if(joinItem.getJoinType() == EJoinType.inner){
					if(_join_Type == null)
						_join_Type = new ArrayList<>();
					_join_Type.add("INNER");
				}
			       
			    TExpression joinCondition = joinItem.getOnCondition();
			    
			    TExpression leftCondition = joinCondition.getLeftOperand();
			    _join_Left_Cond = addJoinCondition(leftCondition,_join_Left_Cond);
				
			    TExpression rightCondition = joinCondition.getRightOperand();
			    _join_Right_Cond = addJoinCondition(rightCondition,_join_Right_Cond);
			}
			if(x == 1)
				executeSelectWithJoin();
			else
				optimizeJoin();
			return;
		}
		executeSelectCommand(selectStmt);
	}
	
	public void optimizeJoin(){
		
	}
	
	public List addJoinCondition( TExpression joinCondition, List joinList){
		if(joinList == null)
			joinList = new ArrayList<>();
		String[] table_col = joinCondition.toString().split("\\.");
		List tableCols = _tableList.get(table_col[0]);
		for (int b = 0; b < tableCols.size(); b++) {
			if (((String) tableCols.get(b)).equalsIgnoreCase(table_col[1])) {
				joinList.add(table_col[0] +"_" + b);
				break;
			}
		}
		return joinList;
	}

	public void executeSelectWithJoin() {
		//TableDetails dual = DBSystem.allTables.get(_select_targetTable_List.get(0));
		String[] table_col_left = _join_Left_Cond.get(0).toString().split("_");
		String[] table_col_right = _join_Right_Cond.get(0).toString().split("_");
		
		TableDetails dual_left = DBSystem.allTables.get(table_col_left[0]);
		HashMap index_left = (HashMap)dual_left.getIndex().get(Integer.parseInt(table_col_left[1]));
		
		TableDetails dual_right = DBSystem.allTables.get(table_col_right[0]);
		HashMap index_right = (HashMap)dual_right.getIndex().get(Integer.parseInt(table_col_right[1]));
		
		//Iterator<Map.Entry<Integer, List>> it = index.entrySet().iterator();
		//while(it.hasNext()){
		File f = new File("./join" + orderByQueryCount);
		f.mkdir(); // create temp directory.*/
		BufferedWriter fos = null;
		try {
			fos = new BufferedWriter(new FileWriter("./join" + orderByQueryCount + "/joinQuery.csv", true));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			for(Object key : index_left.keySet()){
				//Map.Entry<Integer, List> entry = it.next();
				if(index_right.containsKey(key)){
					
					List leftList = (List)index_left.get(key);
					List rightList = (List)index_right.get(key);
					//index_right.containsKey(key)
					String left_record,right_record = null;
					String[] left_record_array, right_record_array = null;
					String tableName = null;
					for(int i = 0; i < leftList.size(); i++){
						left_record = getRecord(table_col_left[0], (int)leftList.get(i));
						left_record_array = left_record.split(",");
						for(int j = 0; j < rightList.size(); j++){
							right_record = getRecord(table_col_left[0], (int)leftList.get(i));
							right_record_array = left_record.split(",");
							for( int d = 0; d < select_Column_Order_List.size(); d++){
								tableName = select_Column_Order_Table_List.get(d).toString();
								if (d != 0)
									fos.append(",");
								if(tableName.equalsIgnoreCase(table_col_left[0])){
									//System.out.println(left_record_array[select_Column_Order_List.get(d)]);
									fos.append(left_record_array[select_Column_Order_List.get(d)]);
								} else if (tableName.equalsIgnoreCase(table_col_right[0])){
									//System.out.println(right_record_array[select_Column_Order_List.get(d)]);
									fos.append(right_record_array[select_Column_Order_List.get(d)]);
								}
							}
							fos.append("\n");
						}
					}
				}	
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (fos != null) {
					fos.flush();
					fos.close();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		
	}
	
	public void executeSelectCommand(TSelectSqlStatement selectStmt) {
		TableDetails dual = DBSystem.allTables.get(_select_TargetTable);
		int totalRecordsInTable = dual.getNumberOfRecords();

		int fileSize = 0, numOfFiles = 0;
		BufferedWriter fos = null;
		int sortColIndex = -1;
		orderByQueryCount ++;

		try {
			
			boolean orderBy = false;
			
			File f = new File("./temp" + orderByQueryCount);
			f.mkdir(); // create temp directory.*/

			fos = new BufferedWriter(new FileWriter("./temp" + orderByQueryCount + "/c_" +  numOfFiles + ".csv", true));
			
			// get order by column value
			if (selectStmt.getOrderbyClause() != null) {
				orderBy = true;
				
				 sortColIndex = (int)_select_OrderBy_Column.get(0);

			}
			
			List<RecordLine> listOfLines = new ArrayList<RecordLine>();

			for (int i = 0; i <= totalRecordsInTable; i++) {
				String record = getRecord(_select_TargetTable, i);
				String[] cols = record.split(",");
				if(cols.length <= 2)
					continue;
				for (int a = 0; a < cols.length; a++) {
					cols[a] = cols[a].trim();
					if (cols[a].startsWith("\"") && cols[a].endsWith("\""))
						cols[a] = cols[a].substring(1, cols[a].length() - 1);
				}

				if (checkForWhereCondition(cols)) {
					if(orderBy){
						//totalBytes += fileSize + record.getBytes().length;
						//if (fileSize + record.getBytes().length > 400) {
						if (fileSize + record.getBytes().length > _pageSize) {
							writeFromListToFile(fos, listOfLines);
							fos.flush();
							fos.close();
							numOfFiles++;
							fos = new BufferedWriter(new FileWriter("./temp"  + orderByQueryCount + "/c_"	+ numOfFiles + ".csv", true));
							fileSize = writeToList(0, listOfLines, record, numOfFiles, sortColIndex);
						}
						else {
							// Write all records in a tree map
							// ordered by key as orderBy column
							// For String column
							fileSize = writeToList(fileSize, listOfLines, record, numOfFiles, sortColIndex);
						}
					}
					else {
						//String[] cols = record.split(",");
						//System.out.print("\n");
						try{
						//for (int j = 0; j < _select_Column_order.length; j++) {
						for (int j = 0; j < select_Column_Order_List.size(); j++) {
							if (j != 0)
								fos.append(",");
							//fos.append(cols[_select_Column_order[j]]);
							fos.append(cols[select_Column_Order_List.get(j)]);
						}
						fos.append("\n");
						}catch(Exception e){
							System.out.println("Error for record : " + i + ": " + record);
						}
					}
				}
			}
			
			//only one file created, then sort it and write to temp file
			if(orderBy){
				writeFromListToFile(fos, listOfLines);
				fos.flush();
				fos.close();
				
		//		if(numOfFiles > 0){
					mergeFiles(numOfFiles);
				//}
			}else {
				fos.flush();
				fos.close();
			}

			
		} catch (IOException io) {
			io.printStackTrace();

		} finally {
			/*try {
				
				if (fos != null) {
					fos.flush();
					fos.close();
				}
		} catch (Exception ex) {
				ex.printStackTrace();
			}*/
		}
	}

	/**
	 * Write to tree map
	 * @param fileSize
	 * @param listOfLines
	 * @param record
	 * @param sortColIndex 
	 * @param numOfFiles 
	 * @param cols
	 * @return
	 */
	private int writeToList(int fileSize, List<RecordLine> listOfLines,
			String record, int numOfFiles, int sortColIndex) {
		
		listOfLines.add(new RecordLine(record, numOfFiles, sortColIndex));
		fileSize += record.getBytes().length;
		return fileSize;
	}

	/**
	 * From tree map write into file
	 * @param fos
	 * @param listOfLines
	 * @param record
	 * @param cols
	 * @throws IOException
	 */
	private void writeFromListToFile(BufferedWriter fos,
			List<RecordLine> listOfLines)	throws IOException {
		
		Collections.sort(listOfLines, new RecordLine((int)_select_OrderBy_Column.get(0)));		
		
			for (RecordLine rec : listOfLines) {
				fos.write(rec.getLine() + "\n");
			}
		listOfLines.clear();

		
	}

/**
 * Merge all individual sorted temp files
 * @param numOfFiles
 */
	private void mergeFiles(int numOfFiles) {
		try {
			ArrayList<FileReader> listOfFileReader = new ArrayList<FileReader>();
			ArrayList<BufferedReader> listOfBufferedReader = new ArrayList<BufferedReader>();
			
			for (int index = 0; index <= numOfFiles; index++) {
				String fileName = "./temp" + orderByQueryCount + "/c_" + index + ".csv";
				listOfFileReader.add(new FileReader(fileName));
				listOfBufferedReader.add(new BufferedReader(listOfFileReader
						.get(index)));
			}
			
			sortFilesAndWriteOutput(listOfBufferedReader);

			for (int index = 0; index < listOfBufferedReader.size(); index++) {
				listOfBufferedReader.get(index).close();
				listOfFileReader.get(index).close();
			}
		} catch (IOException ex) {

		}
	}

	/**
	 * This will sort all files by reading first line of each files
	 * 
	 * @param listOfBufferedReader
	 *            this is list of BufferedReader for all temporary files
	 */
	private void sortFilesAndWriteOutput(List<BufferedReader> listOfBufferedReader) {
		
		try {
			List<RecordLine> listOfLinesfromAllFiles = new ArrayList<RecordLine>();
			int sortColIndex = (int)_select_OrderBy_Column.get(0);

			// Read first line from each file
			for (int index = 0; index < listOfBufferedReader.size(); index++) {
				String line = listOfBufferedReader.get(index).readLine();
				if (line != null) {
					
					listOfLinesfromAllFiles.add(new RecordLine(line, index, sortColIndex));
				}
			}

			FileWriter fw = new FileWriter("./temp" + orderByQueryCount + "/Records_sorted.csv");
			BufferedWriter bw = new BufferedWriter(fw);
			while (true) {
				if (listOfLinesfromAllFiles.size() == 0) {
					break;
				} else {
					Collections.sort(listOfLinesfromAllFiles, new RecordLine(sortColIndex));
					RecordLine recordLine = listOfLinesfromAllFiles.get(0);
					//bw.append(recordLine.getLine() + "\n");
					String[] cols = recordLine.getLine().split(",");
					//System.out.print("\n");
					//for (int j = 0; j < _select_Column_order.length; j++) {
					for (int j = 0; j < select_Column_Order_List.size(); j++) {
						if (j != 0)
							/*System.out.print(",");
						System.out.print(cols[j]);*/
							bw.append(",");
						//bw.append(cols[_select_Column_order[j]]);
						bw.append(cols[select_Column_Order_List.get(j)]);
					}
					bw.append("\n");
					//System.out.print("\n" +recordLine.getLine());
					int indexForFileName = recordLine.getIndexForFileName();

					// Remove read line
					listOfLinesfromAllFiles.remove(0);

					String line = listOfBufferedReader.get(indexForFileName)
							.readLine();
					if (line != null) {
						listOfLinesfromAllFiles.add(new RecordLine(line,indexForFileName, sortColIndex));
					} else {// assume current file which have least amount has
						// no more line, then need to check for further
						// files
						for (int index = 0; index < listOfBufferedReader.size(); index++) {
							if (index == indexForFileName) {
								continue;
							} else {
								line = listOfBufferedReader.get(index)
										.readLine();
								if (line != null) {
									listOfLinesfromAllFiles.add(new RecordLine(line, index, sortColIndex));
									continue;
								}
							}
						}
					}
				}
			}
			bw.flush();
			bw.close();
			fw.close();

		} catch (Exception ex) {

		}
	}

	public void getIntentiveRecords() {
		List allRecords = new ArrayList();
		HashMap map = allTables.get(_select_TargetTable).getIndex();
		for (int i = 0; i < _select_where_Column.size(); i++) {
			HashMap colMap = (HashMap) map.get(_select_where_Column.get(i));
			colMap.get(_select_where_Column_Value.get(i));

		}
	}

	public boolean checkForWhereCondition(String[] cols) {
		if (!_has_Where_Cond)
			return true;
		List allChecks = new ArrayList();
		for (int i = 0; i < _select_where_Column.size(); i++) {
			if (((String) _select_where_Column_cond.get(i)).equals("=")) {
				// System.out.println(cols[(int)_select_where_Column.get(i)]);
				// System.out.println((String)_select_where_Column_Value.get(i));
				if (!cols[(int) _select_where_Column.get(i)]
						.equalsIgnoreCase((String) _select_where_Column_Value
								.get(i))) {
					allChecks.add(0);
				} else {
					allChecks.add(1);
				}
			} else if (((String) _select_where_Column_cond.get(i)).equals("!=")) {
				if (cols[(int) _select_where_Column.get(i)]
						.equalsIgnoreCase((String) _select_where_Column_Value
								.get(i))) {
					allChecks.add(0);
				} else {
					allChecks.add(1);
				}
			} else if (((String) _select_where_Column_cond.get(i))
					.equals("like")) {
				// System.out.println("COMING");
				//if (!cols[(int) _select_where_Column.get(i)].startsWith(((String) _select_where_Column_Value.get(i)))) {
				if (!cols[(int) _select_where_Column.get(i)].toLowerCase().startsWith(((String) _select_where_Column_Value.get(i)).toLowerCase())) {
					allChecks.add(0);
				} else {
					allChecks.add(1);
				}
			} else if (((String) _select_where_Column_cond.get(i)).equals(">=")) {
				if (Integer.parseInt(cols[(int) _select_where_Column.get(i)]) < Integer
						.parseInt((String) _select_where_Column_Value.get(i))) {
					allChecks.add(0);
				} else {
					allChecks.add(1);
				}

			} else if (((String) _select_where_Column_cond.get(i)).equals("<=")) {
				if (Integer.parseInt(cols[(int) _select_where_Column.get(i)]) > Integer
						.parseInt((String) _select_where_Column_Value.get(i))) {
					allChecks.add(0);
				} else {
					allChecks.add(1);
				}
			}
		}
		int k = 0;
		int result = (int) allChecks.get(k);
		// checking from right to left
		if (_select_where_Multi_Connector != null) {
			for (int j = 0; j < _select_where_Multi_Connector.size(); j++) {
				if (((String) _select_where_Multi_Connector.get(j))
						.equalsIgnoreCase("and")) {
					result = result & (int) allChecks.get(++k);
				} else {
					result = result | (int) allChecks.get(++k);
				}
			}
		}
		return result == 1;
	}

	public void getWhereConditions(TSelectSqlStatement selectStmt) {
		getExpression(selectStmt.getWhereClause().getCondition()
				.getLeftOperand(), selectStmt.getWhereClause().getCondition()
				.getRightOperand());
	}

	public TExpression getExpression(TExpression leftExp, TExpression RightExp) {
		if (RightExp.getOperatorToken() == null
				&& leftExp.getOperatorToken() == null) {
			String col = RightExp.getParentExpr().getLeftOperand().toString();
			// System.out.print("\n" + col);
			List tableCols = _tableList.get(_select_TargetTable);

			for (int b = 0; b < tableCols.size(); b++) {
				if (((String) tableCols.get(b)).equalsIgnoreCase(col)) {
					if (_select_where_Column == null)
						_select_where_Column = new ArrayList();
					_select_where_Column.add(b);
					break;
				}
			}

			String op = RightExp.getParentExpr().getOperatorToken().toString();
			// System.out.print(op);
			if (_select_where_Column_cond == null)
				_select_where_Column_cond = new ArrayList();
			_select_where_Column_cond.add(op);

			String value = RightExp.getParentExpr().getRightOperand()
					.toString();
			if (value.endsWith("'") && value.startsWith("'"))
				value = value.substring(1, value.length() - 1);
			// System.out.print(value);
			if (_select_where_Column_Value == null)
				_select_where_Column_Value = new ArrayList();
			_select_where_Column_Value.add(value);
			return null;
		}

		getExpression(RightExp.getLeftOperand(), RightExp.getRightOperand());
		String multiCondConnector = RightExp.getParentExpr().getOperatorToken()
				.toString();
		// System.out.println(multiCondConnector);
		if (_select_where_Multi_Connector == null)
			_select_where_Multi_Connector = new ArrayList();
		_select_where_Multi_Connector.add(multiCondConnector);

		getExpression(leftExp.getLeftOperand(), leftExp.getRightOperand());
		return null;
	}
	
	public int V(String tableName, String colName){
		TableDetails dual = DBSystem.allTables.get(tableName);
		List colList = _tableList.get(tableName);
		int i = 0;
		for(; i < colList.size(); i ++){
			if(colList.get(i).toString().equalsIgnoreCase(colName))
				break;
		}
		HashMap index = dual.getIndex();
		return ((HashMap)index.get(i)).size();
	}
}
