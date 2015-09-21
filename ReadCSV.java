import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ReadCSV {

	public static TableDetails readCSV(String file) {
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		HashMap<Integer,RecordDetails> recordDetailHash = new HashMap<Integer,RecordDetails>();
		HashMap<Integer,PageDetails> pageDetail = new HashMap<Integer,PageDetails>();
		 
		TableDetails tableDetails = new TableDetails();
		byte[] cbuf = new byte[DBSystem._pageSize];
		byte[] rbuf = new byte[DBSystem._pageSize];
		int i;
		int recordNumber = -1;
		int recordInPage = 0;
		int pageNumber = 0;
		long pageStartIndex = 0;
		int fileIndex = 0;
		HashMap indexMap = null;
		int noOfColsInTable = DBSystem._tableList.get(file).size();

		try {
			fis = new FileInputStream(file + ".csv");

			int bufferPointer = 0;
			int recordBufferPointer = 0;
			RecordDetails record = null;
			bis = new BufferedInputStream(fis);
			int lastRecordEnd = 0;
			//int recordsCounter = -1;
			while((i =bis.read())!=-1){
				fileIndex++;
				int lastIndex = 0;
				cbuf[bufferPointer++] = (byte)i;
				rbuf[recordBufferPointer++] = (byte)i;
				if(bufferPointer == DBSystem._pageSize){
					String page = new String(cbuf);
					lastIndex = page.lastIndexOf("\n");
					page = page.substring(0, lastIndex+1);
					//System.out.println(page);
					Arrays.fill(cbuf, (byte)0);
					Arrays.fill(rbuf, (byte)0);
					recordBufferPointer = 0;
					bufferPointer = 0;
					bis.reset();
					fileIndex = lastRecordEnd;
					pageDetail.put(pageNumber, new PageDetails(pageStartIndex,fileIndex,recordInPage));

					/*byte[] buf = new byte[DBSystem._pageSize];
					try{
						RandomAccessFile raf = new RandomAccessFile(file + ".csv","r");
						raf.seek(pageStartIndex);
						raf.read(buf, 0, (int) (fileIndex - pageStartIndex));
						//System.out.println(new String(buf)); 
					} catch (Exception e) {
						e.printStackTrace();
					}*/
					pageNumber++;
					pageStartIndex = lastRecordEnd;
					recordInPage = 0;
				}
				else if (i == 10) {
					//recordsCounter++;
					recordNumber++;
					recordInPage++;
					String row = new String(rbuf);
					String[] rowHash = row.split(",");
					if(rowHash.length != noOfColsInTable)
						System.out.println("Problem with Row : " + recordNumber + " of Table " + file);
					for(int a=0; a < rowHash.length; a++){
						rowHash[a] = rowHash[a].trim().toLowerCase();
						if(rowHash[a].startsWith("\"") && rowHash[a].endsWith("\"")){
							rowHash[a] = rowHash[a].substring(1,rowHash[a].length()-1);
							if(rowHash[a] == "")
								System.out.println("Blank column for row " + recordNumber + " of Table " + file);
						}
					}
					//System.out.println(row);
					if(recordNumber == 0){
						indexMap = new HashMap<Integer,HashMap>();
						for(int b = 0; b < rowHash.length; b++){
							indexMap.put(b, new HashMap());
						}
					} 
					
					for(int c = 0; c < rowHash.length; c++){
						HashMap map = (HashMap)indexMap.get(c);
						if(map == null)
							continue;
						if(map.containsKey(rowHash[c])){
							List list = (List)map.get(rowHash[c]);
							list.add(recordNumber);
						}else{
							List list = new ArrayList<>();
							list.add(recordNumber);
							map.put(rowHash[c], list);
						}
					}
					
					record = new RecordDetails(pageNumber,recordInPage);
					recordDetailHash.put(recordNumber, record);
					bis.mark(fileIndex);
					lastIndex = fileIndex;
					lastRecordEnd = fileIndex;
					Arrays.fill(rbuf, (byte)0);
					recordBufferPointer = 0;
				}
			}
			pageDetail.put(pageNumber, new PageDetails(pageStartIndex,fileIndex,recordInPage));
			tableDetails.setLastPageOfTable(pageNumber);
			tableDetails.setNumberOfRecords(recordNumber);
			tableDetails.setIndex(indexMap);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (fis != null) {
					fis.close();
				}
				if (bis != null) {
					bis.close();
				}
			} catch (Exception e) {
				System.out.println("");
			}
		}
		tableDetails.setPageDetails(pageDetail);
		tableDetails.setRecordDetails(recordDetailHash);
		return tableDetails;
	}

}
