//package dubstep;
import java.util.Map;

public class TableData {

	Map<String, Integer> columnOrderMapping;
	Map<String, String> columnDataTypeMapping;
	Map<String, Map> columnIndex;

	public Map<String, Integer> getColumnOrderMapping() {
		return columnOrderMapping;
	}

	public void setColumnOrderMapping(Map<String, Integer> columnOrderMapping) {
		this.columnOrderMapping = columnOrderMapping;
	}

	public Map<String, String> getColumnDataTypeMapping() {
		return columnDataTypeMapping;
	}

	public void setColumnDataTypeMapping(Map<String, String> columnDataTypeMapping2) {
		this.columnDataTypeMapping = columnDataTypeMapping2;
	}

	public Map<String, Map> getColumnIndex() {
		return columnIndex;
	}

	public void setColumnIndex(Map<String, Map> columnIndex) {
		this.columnIndex = columnIndex;
	}


}
