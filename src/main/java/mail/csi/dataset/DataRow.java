package mail.csi.dataset;

import mail.csi.DateExt;
import mail.csi.Utils;

import javax.rmi.CORBA.Util;
import java.time.LocalDate;

public class DataRow {
    private String[] row;
    public DataSchema schema;

    public DataRow(DataSchema schema) {
        this.schema = schema;
        row = new String[schema.columns().size()];
    }

    public int getInt(String columnName) {
        String value = get(columnName);
        return Integer.parseInt(value);
    }

    public void addColumn() {
        String[] row2 = new String[row.length + 1];
        System.arraycopy(row, 0, row2, 0, row.length);
        row = row2;
    }

    public void set(String columnName, double value) {
        Integer ind = schema.columnMap().get(columnName);
        if (ind == null) {
            throw new RuntimeException("Column is not found: " + columnName);
        }

        row[ind] = Utils.num(value);
    }

    public double getDouble(String columnName) {
        String valueRaw = get(columnName);
        if (valueRaw == null || valueRaw.length() == 0) {
            return 0d;
        }

        return Utils.getDouble(valueRaw);
    }

    public int yearMon(String columnName) {
        String valueRaw = get(columnName);
        int yearMon = new DateExt(valueRaw).yearMon();

        return yearMon;
    }

    public LocalDate getDate(String columnName) {
        String valueRaw = get(columnName);
        DateExt date = new DateExt(valueRaw);

        return date.getDate();
    }

    public double getDouble(String columnName, double valueDefault) {
        String valueRaw = get(columnName);
        if (valueRaw == null || valueRaw.length() == 0) {
            return valueDefault;
        }

        return Double.parseDouble(valueRaw);
    }

    public String get(String columnName, String valueDefault) {
        String value = get(columnName);
        if (value == null || value.length() == 0) {
            return valueDefault;
        }
        return value;
    }

    public String get(String columnName) {
        Integer ind = schema.columnMap().get(columnName);
        if (ind == null) {
            throw new RuntimeException("Column is not found: " + columnName);
        }

        String value = row[ind];
        return value;
    }

    public String get(int ind) {
        String value = row[ind];
        return value;
    }

    public void put(int ind, String value) {
        row[ind] = value;
    }
}
