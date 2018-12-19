package mail.csi.dataset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by demo on 5/11/2018.
 */
public class DataList {
    private static final Logger log = LoggerFactory.getLogger(DataList.class);

    private int maxRows = Integer.MAX_VALUE;
    public DataSchema schema = new DataSchema();
    private List<DataRow> rows = new ArrayList<>();
    public String fileName = null;

    public DataList() {
    }

    public List<DataRow> rows() {
        return rows;
    }

    public void addColumn(String columnName) {
        schema.columnAdd(columnName);
        for (DataRow row : rows) {
            row.addColumn();
        }
    }

    public void load(String fileName) throws Exception {
        log.warn("Loading file: {}...", fileName);
        this.fileName = fileName;

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line = br.readLine();
            String[] data = line.split(";");

            for(int ind = 0; ind < data.length; ind++) {
                String columnName = data[ind];
                columnName = columnName.toUpperCase();
                columnName = columnName.replace("#", "");

                schema.columnMap().put(columnName, schema.columns().size());
                schema.columns().add(columnName);
            }

            int rowCnt = 0;
            while ((line = br.readLine()) != null) {
                if (line.length() == 0) {
                    continue;
                }

                data = line.split(";");
                if (++rowCnt % 500_000 == 0) {
                    log.warn("Loading file {}, {} rows...", fileName, rowCnt);
                }

                DataRow row = new DataRow(schema);
                rows.add(row);

                int maxInd = Math.min(data.length, schema.columns().size());
                for (int ind = 0; ind < maxInd; ind++) {
                    String value = data[ind];
                    row.put(ind, value);
                }

                if (rowCnt >= maxRows) {
                    break;
                }
            }
        }

        log.warn("Loaded file {}, {} rows...", fileName, rows.size());
    }
}
