
public class PageDetails {

	private long _startIndex;
	private long _endIndex;
	private int _numberOfRecoredsInPage;

	public PageDetails(long startIndex, long endIndex, int numberOfRecoredsInPage){
		this._startIndex = startIndex;
		this._endIndex = endIndex;
		this._numberOfRecoredsInPage = numberOfRecoredsInPage;

	}

	public long getStartIndex() {
		return _startIndex;
	}
	public void setStartIndex(long startIndex) {
		this._startIndex = startIndex;
	}
	public long getEndIndex() {
		return _endIndex;
	}
	public void setEndIndex(long endIndex) {
		this._endIndex = endIndex;
	}
	public int getNumberOfRecoredsInPage() {
		return _numberOfRecoredsInPage;
	}
	public void setNumberOfRecoredsInPage(int numberOfRecoredsInPage) {
		this._numberOfRecoredsInPage = numberOfRecoredsInPage;
	}
}
