package mail.csi.dataset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DataSchema {
    private List<String> columns = new ArrayList<>();
    private HashMap<String, Integer> columnMap = new HashMap<>();

    public List<String> columns() {
        return columns;
    }

    public HashMap<String, Integer> columnMap() {
        return columnMap;
    }

    public Integer columnAdd(String columnName) {
        Integer columnInd = columns.size();
        columnMap.put(columnName, columnInd);
        columns.add(columnName);
        return columnInd;
    }

}
