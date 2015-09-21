
public class RecordDetails {

	private int _pageNumber;
	private int _recordNumberInPage;

	public RecordDetails(int pageNumber, int recordNumberInPage){
		this._pageNumber = pageNumber;
		this._recordNumberInPage = recordNumberInPage;
	}

	public int getRecordNumberInPage() {
		return _recordNumberInPage;
	}

	public void setRecordNumberInPage(int _recordNumberInPage) {
		this._recordNumberInPage = _recordNumberInPage;
	}

	public int getPageNumber() {
		return _pageNumber;
	}
	public void setPageNumber(int pageNumber) {
		this._pageNumber = pageNumber;
	}
}
