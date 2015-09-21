import java.util.HashMap;


public class TableDetails {

	//private HashMap recordDetails = new HashMap();
	//private HashMap pageDetails = new HashMap();
	
	private HashMap recordDetails = null;
	private HashMap pageDetails = null;
	private HashMap index = null;
	private int lastPageOfTable=0;
	private int numberOfRecords = 0;

	public HashMap getIndex() {
		return index;
	}

	public void setIndex(HashMap indexMap) {
		this.index = indexMap;
	}
	
	public int getNumberOfRecords() {
		return numberOfRecords;
	}

	public void setNumberOfRecords(int numberOfRecords) {
		this.numberOfRecords = numberOfRecords;
	}

	/**
	 * 
	 * @return
	 */
	public int getLastPageOfTable() {
		return lastPageOfTable;
	}

	/**
	 * 
	 * @param lastPageOfTable
	 */
	public void setLastPageOfTable(int lastPageOfTable) {
		this.lastPageOfTable = lastPageOfTable;
	}

	/**
	 * 
	 * @return
	 */
	public HashMap getRecordDetails() {
		return recordDetails;
	}

	/**
	 * 
	 * @param <V>
	 * @param recordDetails
	 */
	public void setRecordDetails(HashMap<Integer,RecordDetails> recordDetails) {
		this.recordDetails = recordDetails;
	}

	/**
	 * 
	 * @return
	 */
	public HashMap getPageDetails() {
		return pageDetails;
	}

	/**
	 * 
	 * @param pageDetails
	 */
	public void setPageDetails(HashMap pageDetails) {
		this.pageDetails = pageDetails;
	}
}
