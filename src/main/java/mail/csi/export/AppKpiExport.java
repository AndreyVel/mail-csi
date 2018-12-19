package mail.csi.export;

import mail.csi.*;
import mail.csi.dataset.DataList;
import mail.csi.dataset.DataRow;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDate;
import java.util.*;

/**
 * Created by demo on 5/11/2018.
 */
public class AppKpiExport {
    private static final Logger log = LoggerFactory.getLogger(AppKpiExport.class);
    private static int LAST_DAYS = 28;

    public static void main(String[] arg) throws Exception {
        BasicConfigurator.configure();

        String[] dataTags = new String[]{"train", "test"};
        for (String dataTag : dataTags) {
            UserSci sci = new UserSci();
            sci.loadData(AppConfig.getFileNameIn(dataTag, "subs_csi"));
            HashMap<Integer, HashMap<Integer, CellUserStat>> cell2Users = null;

            cell2Users = getCell2Users(AppConfig.getFileNameIn(dataTag, "subs_bs_data_session"), sci, "DATA_VOL_MB");
            exportData(dataTag, "/pca-chnn-data/pca-chnn-data",
                    AppConfig.getFileNameIn(dataTag, "bs_chnn_kpi"), sci, cell2Users, 2, 34); // 2-34

            exportData(dataTag,  "/pca-avg-data/pca-avg-data",
                    AppConfig.getFileNameIn(dataTag, "bs_avg_kpi"), sci, cell2Users, 2, 41); // 2-41

            cell2Users = getCell2Users(AppConfig.getFileNameIn(dataTag,"subs_bs_voice_session"), sci, "VOICE_DUR_MIN");
            exportData(dataTag, "/pca-chnn-voice/pca-chnn-voice",
                    AppConfig.getFileNameIn(dataTag, "bs_chnn_kpi"), sci, cell2Users, 2, 34); // 2-34

            exportData(dataTag, "/pca-avg-voice/pca-avg-voice",
                    AppConfig.getFileNameIn(dataTag, "bs_avg_kpi"), sci, cell2Users, 2, 41); // 2-41
        }
    }

    public static HashMap<Integer, HashMap<Integer, CellUserStat>> getCell2Users(String fileName, UserSci sci, String valueCol) throws Exception {
        DataList dataList = new DataList();
        dataList.load(fileName);

        HashMap<Integer, HashMap<Integer, CellUserStat>> cell2Users = new HashMap<>();
        HashSet<Integer> notFoundUserInData = new HashSet<>();

        for (DataRow row : dataList.rows()) {
            int userId = row.getInt("SK_ID");
            RowSci userSci = sci.map.get(userId);
            if (userSci == null) {
                notFoundUserInData.add(userId);
                continue;
            }

            double value = row.getDouble(valueCol);
            int cellId = row.getInt("CELL_LAC_ID");
            LocalDate sesDate = row.getDate("START_TIME");

            HashMap<Integer, CellUserStat> userMap = cell2Users.get(cellId);
            if (userMap == null) {
                userMap = new HashMap<>();
                cell2Users.put(cellId, userMap);
            }

            CellUserStat stat = userMap.get(userId);
            if (stat == null) {
                stat = new CellUserStat(userSci);
                userMap.put(userId, stat);
            }
            stat.update(sesDate, value);
        }

        log.warn("notFoundUserInData={} / {}", notFoundUserInData.size(), sci.rows().size());
        return cell2Users;
    }

    public static class SessionStat {
        public int sessionNum = 0;
        public double valueSes = 0;
        public double valueKpi = 0;
    }

    public static class SessionStat2 {
        public ValueAgg agg = new ValueAgg();
        public double sessionNum = 0;
        public double valueSes = 0;

        public void update(SessionStat stat) {
            sessionNum = stat.sessionNum;
            valueSes = stat.valueSes;
            agg.update(stat.valueKpi);
        }

        public static void header(BufferedWriter writer, int day) throws IOException {
            writer.write(";A" + day + ";B" + day + ";C" + day + ";D" + day + ";E" + day);
        }

        public static void values(BufferedWriter writer, SessionStat2 stat2) throws IOException {
            if (stat2 == null) {
                writer.write(";0;0;0;0;0");
            } else {
                double avg = stat2.agg.avg2();
                writer.write(";" + Utils.num(avg));
                writer.write(";" + Utils.num(avg * stat2.valueSes));
                writer.write(";" + Utils.num(avg * stat2.sessionNum));

                writer.write(";" + Utils.div(stat2.valueSes, avg));
                writer.write(";" + Utils.div(stat2.sessionNum, avg));
            }
        }
    }

    public static class CellUserStat {
        public HashMap<LocalDate, SessionStat> sesDayMap = new HashMap<>();
        public HashMap<LocalDate, SessionStat> valueMap = new HashMap<>();
        public RowSci userSci;

        public CellUserStat(RowSci userSci) {
            this.userSci = userSci;
        }

        public void update(LocalDate sesDate, double value) {
            SessionStat stat = sesDayMap.get(sesDate);
            if (stat == null) {
                stat = new SessionStat();
                sesDayMap.put(sesDate, stat);
            }
            stat.sessionNum++;
            stat.valueSes += value;
        }

        public void clearStat() {
            valueMap.clear();
        }

        public void updateStat(LocalDate valueKpi, double value) {
            SessionStat stat = sesDayMap.get(valueKpi);
            if (stat != null) {
                SessionStat stat2 = valueMap.get(valueKpi);
                if (stat2 != null) {
                    throw new RuntimeException("err");
                }

                stat2 = new SessionStat();
                stat2.sessionNum = stat.sessionNum;
                stat2.valueSes = stat.valueSes;
                stat2.valueKpi = value;

                valueMap.put(valueKpi, stat2);
            }
        }
    }

    public static void exportData(String dataTag, String exportTag, String sourceFile, UserSci sci,
                                  HashMap<Integer, HashMap<Integer, CellUserStat>> cell2Users, int indFrom, int indTo) throws Exception {

        for (int colInd = indFrom; colInd <= indTo; colInd++) {
            String fileExpPath = AppConfig.getFileNameOut(dataTag, exportTag + colInd);
            File fileExp = new File(fileExpPath);

            if (!fileExp.getParentFile().exists()) {
                fileExp.getParentFile().mkdirs();
            }

            if (fileExp.exists()) {
                log.warn("File exists: {}...", fileExpPath);
                continue;
            }

            clearStat(cell2Users);
            calculateStat(sourceFile, cell2Users, colInd);

            HashMap<Integer, HashMap<LocalDate, SessionStat2>> user2Days = new HashMap<>();
            HashMap<Integer, SessionStat2> userStat2 = new HashMap<>();

            for (HashMap<Integer, CellUserStat> userMap : cell2Users.values()) {
                for (Map.Entry<Integer, CellUserStat> pairUser : userMap.entrySet()) {
                    CellUserStat ustat = pairUser.getValue();
                    int userId = pairUser.getKey();

                    SessionStat2 statTot = userStat2.get(userId);
                    if (statTot == null) {
                        statTot = new SessionStat2();
                        userStat2.put(userId, statTot);
                    }

                    for (LocalDate date : ustat.valueMap.keySet()) {
                        HashMap<LocalDate, SessionStat2> userDays = user2Days.get(userId);
                        if (userDays == null) {
                            userDays = new HashMap<>();
                            user2Days.put(pairUser.getKey(), userDays);
                        }

                        SessionStat2 stat2 = userDays.get(date);
                        if (stat2 == null) {
                            stat2 = new SessionStat2();
                            userDays.put(date, stat2);
                        }

                        SessionStat stat = ustat.valueMap.get(date);
                        statTot.update(stat);
                        stat2.update(stat);
                    }
                }
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileExpPath))) {
                writer.write("CSI;SK_ID");
                SessionStat2.header(writer, 100);

                for (int day = 0; day < LAST_DAYS; day++) {
                    SessionStat2.header(writer, day);
                }
                writer.write(System.lineSeparator());

                for (RowSci row : sci.rows()) {
                    HashMap<LocalDate, SessionStat2> userDays = user2Days.get(row.SK_ID);
                    if (userDays == null) {
                        continue;
                    }

                    writer.write(row.CSI + ";" + row.SK_ID);
                    SessionStat2 statTot = userStat2.get(row.SK_ID);
                    SessionStat2.values(writer, statTot);

                    for (int day = 0; day < LAST_DAYS; day++) {
                        LocalDate date = row.CONTACT_DATE.getDate().minusDays(day);
                        SessionStat2 stat2 = userDays.get(date);
                        SessionStat2.values(writer, stat2);
                    }
                    writer.write(System.lineSeparator());
                }
            }
        }
    }

    public static void clearStat(HashMap<Integer, HashMap<Integer, CellUserStat>> cell2Users) {
        for (HashMap<Integer, CellUserStat> userMap : cell2Users.values()) {
            for (CellUserStat stat : userMap.values()) {
                stat.clearStat();
            }
        }
    }

    public static void calculateStat(String fileName, HashMap<Integer, HashMap<Integer, CellUserStat>> cell2Users, int colInd) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line = br.readLine();
            String[] data = line.split(";");
            if (colInd > data.length) {
                throw new IllegalArgumentException("indTo > data.length");
            }

            int rowCnt = 0;
            while ((line = br.readLine()) != null) {
                data = line.split(";");
                if (++rowCnt % 500_000 == 0) {
                    log.warn("Loading file {}, {}/{} rows...", fileName, colInd, rowCnt);
                }

                if (colInd >= data.length) {
                    continue;
                }

                LocalDate dateKpi = new DateExt(data[0]).getDate();
                int cellId = Integer.parseInt(data[1]);
                double valueKpi = Utils.getDouble(data[colInd]);
//                if (cellId != 288009) {
//                    continue;
//                }

                HashMap<Integer, CellUserStat> userMap = cell2Users.get(cellId);
                if (userMap == null) {
                    continue;
                }

                for (CellUserStat stat : userMap.values()) {
                    stat.updateStat(dateKpi, valueKpi);
                }
            }
        }
    }
}
