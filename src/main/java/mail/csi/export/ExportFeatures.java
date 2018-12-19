package mail.csi.export;

import mail.csi.*;
import mail.csi.dataset.DataList;
import mail.csi.dataset.DataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class ExportFeatures {
    private static final Logger log = LoggerFactory.getLogger(AppExport.class);

    public static void execute(String filePath, UserSci mapSci, DataList featureList) throws Exception {
        log.warn("Export file: {}", filePath);
        featureList.addColumn("PAY_SUM");

        FeatureStat totalAllStat = new FeatureStat();
        HashMap<Integer, FeatureStat> featureStatMap = new HashMap<>();
        HashMap<String, DataRow> featureStatMon = new HashMap<>();

        List<String> exportCols = new ArrayList<>();
        for (String col : featureList.schema.columns()) {
            if ("SNAP_DATE".equals(col)) continue;
            if ("SK_ID".equals(col)) continue;
            exportCols.add(col);
        }

        for (DataRow row : featureList.rows()) {
            Integer SK_ID = row.getInt("SK_ID");
            LocalDate SNAP_DATE = new DateExt(row.get("SNAP_DATE")).getDate();

            double valueSum = row.getDouble("ITC");
            valueSum += row.getDouble("VAS");
            valueSum += row.getDouble("RENT_CHANNEL");
            valueSum += row.getDouble("ROAM");
            row.set("PAY_SUM", valueSum);

            RowSci rowSci = mapSci.map.get(SK_ID);
            if (rowSci.CONTACT_DATE.getDate().isBefore(SNAP_DATE)) {
                throw new RuntimeException("err");
            }

            FeatureStat userStat = featureStatMap.get(SK_ID);
            if (userStat == null) {
                userStat = new FeatureStat();
                featureStatMap.put(SK_ID, userStat);
            }
            userStat.update(exportCols, row);

            totalAllStat.update(exportCols, row);
            featureStatMon.put(row.get("SK_ID") + "$" + Utils.yearMon(SNAP_DATE), row);
        }

        int countSkip = 0;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write("CSI;SK_ID;IS_NEW");

            for (String col : exportCols) {
                writer.write(";" + col + "_LAST");
            }

            FeatureStat.writeHeader(writer, exportCols, "_YEAR");
            FeatureStat.writeHeaderDx(writer, exportCols, "_DX1");
            FeatureStat.writeHeaderDx(writer, exportCols, "_DX2");
            FeatureStat.writeHeaderDx(writer, exportCols, "_DX3");
            writer.write(System.lineSeparator());

            for (RowSci row : mapSci.rows()) {
                FeatureStat totalUserStat = featureStatMap.get(row.SK_ID);

                DataRow featureLast = null;
                if (row.CONTACT_DATE.DAY >= 15) {
                    int yearMon = Utils.yearMon(row.CONTACT_DATE.getDate());
                    featureLast = featureStatMon.get(row.SK_ID + "$" + yearMon);
                }
                if (featureLast == null) {
                    int yearMon = Utils.yearMon(row.CONTACT_DATE.getDate().minusMonths(1));
                    featureLast = featureStatMon.get(row.SK_ID + "$" + yearMon);
                }

                if (featureLast == null) {
                    for (int monPrev = 0; monPrev <= totalUserStat.map.size(); monPrev++) {
                        int yearMon = Utils.yearMon(row.CONTACT_DATE.getDate().minusMonths(monPrev));
                        featureLast = featureStatMon.get(row.SK_ID + "$" + yearMon);
                        if (featureLast != null) break;
                    }
                }

                if (featureLast == null) {
                    throw new Exception("err");
                }

                writer.write(row.CSI + ";");
                writer.write(row.SK_ID + ";");
                writer.write((totalUserStat.map.size() <= 3 ? "1" : "0"));

                for (String col : exportCols) {
                    double value = featureLast.getDouble(col);
                    if (value == 0) {
                        value = totalUserStat.map.get(col).avg();
                    }
                    if (value == 0) {
                        value = totalAllStat.map.get(col).avg();
                    }
                    writer.write(";" + Utils.num(value));
                }

                totalUserStat.writeValue(writer, exportCols, totalAllStat);
                totalUserStat.writeValueDx(writer, exportCols, totalAllStat);

                totalAllStat.writeValueDx(writer, exportCols, featureLast);
                totalUserStat.writeValueDx(writer, exportCols, featureLast);

                writer.write(System.lineSeparator());
            }
            log.warn("Export {}, countSkip={}", filePath, countSkip++);
        }
    }

    public static class FeatureStat {
        HashMap<String, ValueAgg> map = new HashMap<>();
        HashSet<Integer> yearMon = new HashSet<>();

        public void update(List<String> cols, DataRow row) {
            DateExt dateMon = new DateExt(row.get("SNAP_DATE"));
            yearMon.add(dateMon.yearMon());

            for (String col : cols) {
                ValueAgg agg = map.get(col);
                if (agg == null) {
                    agg = new ValueAgg();
                    map.put(col, agg);
                }
                agg.update(row.getDouble(col));
            }
        }

        public static void writeHeader(BufferedWriter writer, List<String> cols, String tag) throws IOException {
            for (String col : cols) {
                writer.write(";" + col + tag + "1");
                writer.write(";" + col + tag + "2");
                writer.write(";" + col + tag + "3");
            }
        }

        public void writeValue(BufferedWriter writer, List<String> cols, FeatureStat featureAll) throws IOException {
            for (String col : cols) {
                ValueAgg agg = map.get(col);
                if (agg == null) {
                    agg = featureAll.map.get(col);
                }
                writer.write(";" + Utils.num(agg.avg2()));
                writer.write(";" + Utils.num(agg.uniqueMod()));
                writer.write(";" + Utils.num(agg.uniqueNum()));
            }
        }

        public static void writeHeaderDx(BufferedWriter writer, List<String> cols, String tag) throws IOException {
            for (String col : cols) {
                writer.write(";" + col + tag + "1");
            }
        }

        public void writeValueDx(BufferedWriter writer, List<String> cols, DataRow featureLast) throws IOException {
            for (String col : cols) {
                ValueAgg agg2 = map.get(col);
                double value = featureLast.getDouble(col);

                String str = "0";
                if (agg2 != null && value != 0) {
                    str = Utils.numDiv(value, agg2.avg2());
                }
                writer.write(";" + str);
            }
        }

        public void writeValueDx(BufferedWriter writer, List<String> cols, FeatureStat stat) throws IOException {
            for (String col : cols) {
                ValueAgg agg = map.get(col);
                ValueAgg agg2 = stat.map.get(col);

                String str = "0";
                if (agg != null && agg2 != null) {
                    str = Utils.numDiv(agg.avg2(), agg2.avg2());
                }
                writer.write(";" + str);
            }
        }
    }
}
