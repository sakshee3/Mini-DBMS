import java.util.Comparator;

public class RecordLine implements Comparator<RecordLine> {
	// Presently not used

	private String line;

	private Class compareClass;

	private int indexForFileName;
	private int sortColIndex;

	public RecordLine(int sortColIndex) {
		if("varchar".equalsIgnoreCase(DBSystem._colTypeList.get(DBSystem._select_TargetTable).get(sortColIndex))){
			this.compareClass = String.class;
		}
		else {
			this.compareClass = Integer.class;
		}

	}

	public int compare(RecordLine o1, RecordLine o2) {
		int colValue1 = 0;
		int colValue2 = 0;
		if (compareClass == String.class) {
			return o1.line.split(",")[o1.sortColIndex].compareTo(o2.line.split(",")[o2.sortColIndex]);
		} else {
			try{
				String col1 = o1.line.split(",")[o1.sortColIndex];
				String col2 = o2.line.split(",")[o2.sortColIndex];
				colValue1 = Integer.parseInt(format(col1));
				colValue2 = Integer.parseInt(format(col2));
			}catch(Exception e){
				System.out.println("ERROR");
			}
			if(colValue1 > colValue2)
				return 1;
			else if(colValue1 < colValue2)
				return -1;
			else
				return 0;
			
		} 
		
		
	}

	public String format(String col){
		if(col.startsWith("\"") && col.endsWith("\""))
			return col.substring(1, col.length()-1);
		return col;
	}
	
	public RecordLine(String line, int indexForFileName, int sortColIndex) {
		this.line = line;
		this.indexForFileName = indexForFileName;
		this.sortColIndex = sortColIndex;
	}
	
	public RecordLine(String record) {
		this.line = record;
	}

	public String getLine(){
		return line;
	}

	@Override
	public boolean equals(Object other) {
		if (other == null)
			return false;
		if (other == this)
			return true;
		if (this.getClass() != other.getClass())
			return false;
		RecordLine otherMyClass = (RecordLine) other;
		return otherMyClass.line.equals(this.line);
	}

	@Override
	public int hashCode() {
		return this.line.hashCode();
	}

	public int getIndexForFileName() {
		return indexForFileName;
	}
}
