package dubstep;
import java.util.List;
import java.util.Map;

public class TableData {

	String tableName;
	Map<String, Integer> columnOrderMapping;
	Map<String, String> columnDataTypeMapping;
	Map<String, Map> columnIndex;
	Map<Long, String[]> primaryKeyIndex;
	List<String> primaryKeyList;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Map<Long, String[]> getPrimaryKeyIndex() {
		return primaryKeyIndex;
	}

	public void setPrimaryKeyIndex(Map<Long, String[]> primaryKeyIndex) {
		this.primaryKeyIndex = primaryKeyIndex;
	}
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

	public List<String> getPrimaryKeyList() {
		return primaryKeyList;
	}

	public void setPrimaryKeyList(List<String> primaryKeyList) {
		this.primaryKeyList = primaryKeyList;
	}


}
