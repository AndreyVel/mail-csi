package mail.csi.export;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import mail.csi.DateExt;
import mail.csi.RowSci;
import mail.csi.UserSci;
import mail.csi.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.time.LocalDate;
import java.util.*;

/**
 * Created by demo on 5/11/2018.
 */
public class SubsBsSession {
    private static final Logger log = LoggerFactory.getLogger(SubsBsSession.class);
    private List<DataRow> list = new ArrayList();
    private int maxRows = Integer.MAX_VALUE;

    private StatUser tatalStat = new StatUser();
    public HashMap<Integer, StatUser> userStatMap = new HashMap<>();
    public HashMap<String, StatUser> userMonMap = new HashMap<>();

    public List<DataRow> rows() {
        return list;
    }

    public SubsBsSession() {
    }

    public SubsBsSession(int maxRows) {
        this.maxRows = maxRows;
    }

    public void calcStat(UserSci sci) {
        // В таблице содержится информация времени и продолжительности DATA/VOICE-сессий абонентов в привязке к сотам,
        // за последние два месяца до опроса CSI. Временная гранулярность  = сессия. Уровень аграгации =  абонент/сота
        // SK_ID, CELL_LAC_ID, {DATA_VOL_MB || VOICE_DUR_MIN}, START_TIME

        for (DataRow row : list) {
            RowSci userRow = sci.map.get(row.SK_ID);
            LocalDate dateOpr = userRow.CONTACT_DATE.getDate();

            LocalDate dateSes = row.START_TIME.getDate();
            if (dateOpr.isBefore(dateSes)) {
                throw new RuntimeException("errr");
            }

            String monKey = row.SK_ID + "$" + row.START_TIME.yearMon();
            StatUser monStat = userMonMap.get(monKey);
            if (monStat == null) {
                monStat = new StatUser();
                userMonMap.put(monKey, monStat);
            }
            monStat.update(row);

            StatUser userStat = userStatMap.get(row.SK_ID);
            if (userStat == null) {
                userStat = new StatUser();
                userStatMap.put(row.SK_ID, userStat);
            }
            userStat.update(row);
            tatalStat.update(row);
        }
    }

    public void exportSession(String filePath, UserSci sci, String prefix) throws Exception {
        log.warn("Export file: {}", filePath);
        calcStat(sci);

        int countSkip = 0;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write("CSI;SK_ID");

            writer.write(headers(prefix));
            writer.write(System.lineSeparator());

            for (RowSci row : sci.rows()) {
                String values = value(row.SK_ID);
                if (values == null) {
                    log.warn("No session: SK_ID={}, countSkip={}", row.SK_ID, countSkip++);
                    continue;
                }

                writer.write(row.CSI + ";" + row.SK_ID);
                writer.write(values); // Voice sessions
                writer.write(System.lineSeparator());
            }
        }
    }

    public ArrayList<Integer> topCells(int SK_ID, int topNum) {
        SubsBsSession.StatUser userStat = userStatMap.get(SK_ID);
        if (userStat.cellMap.size() == 0) {
            throw new RuntimeException("err");
        }

        ArrayList<StatCell> listCels = new ArrayList(userStat.cellMap.values());
        listCels.sort((t0, t1) -> Integer.compare(t1.counter, t0.counter));

        Random rand = new Random();
        ArrayList<Integer> topMap = new ArrayList<>();
        for (int ind = 0; ind < topNum; ind++) {
            if (ind < listCels.size()) {
                int cellId = listCels.get(ind).CELL_LAC_ID;
                topMap.add(cellId);
            } else {
                int randInd = rand.nextInt(topMap.size());
                topMap.add(topMap.get(randInd));
            }
        }

        return topMap;
    }

    public String headers(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(";" + prefix + "DAYS");
        sb.append(";" + prefix + "CELL");
        sb.append(";" + prefix + "SESZ");
        sb.append(";" + prefix + "SESS");

        sb.append(";" + prefix + "VAL_AVGD");
        sb.append(";" + prefix + "VAL_AVGS");
        sb.append(";" + prefix + "HOUR_AVGS");

        StatUser.writeHeader(sb, prefix + "MDX");
        StatUser.writeHeader(sb, prefix + "TOT");
        return sb.toString();
    }

    public String value(int userId) {
        StringBuilder sb = new StringBuilder();
        StatUser stat = userStatMap.get(userId);
        if (stat == null) {
            return null;
        }

        int days = stat.daySet.size();
        sb.append(";" + days);
        sb.append(";" + stat.cellSet.size());
        sb.append(";" + Utils.numDiv(stat.zerroCnt, days));
        sb.append(";" + Utils.numDiv(stat.sessionCnt, days));

        sb.append(";" + Utils.numDiv(stat.totalValue, days));
        sb.append(";" + Utils.numDiv(stat.totalValue, stat.sessionCnt));
        sb.append(";" + Utils.numDiv(stat.totalHours, stat.sessionCnt));

        String monKey = userId + "$" + stat.lastYearMon;
        StatUser statLast = userMonMap.get(monKey);

        statLast.writeValues(sb, stat);
        tatalStat.writeValues(sb, stat);

        return sb.toString();
    }

    public static class StatCell {
        public int CELL_LAC_ID;
        public int counter = 0;
        public double tatalValue = 0;

        public StatCell(int CELL_LAC_ID) {
            this.CELL_LAC_ID = CELL_LAC_ID;
        }

        public void update(DataRow row) {
            tatalValue += row.VALUE;
            counter++;
        }
    }

    public static class StatUser {
        public int zerroCnt = 0;
        public int sessionCnt = 0;
        public int totalHours = 0;
        public double totalValue = 0;
        public int lastYearMon = 0;

        public HashSet<Integer> cellSet = new HashSet<>();
        public HashSet<LocalDate> daySet = new HashSet<>();
        public HashMap<Integer, StatCell> cellMap = new HashMap<>();


        public void update(DataRow row) {
            sessionCnt++;
            totalValue += row.VALUE;
            totalHours += row.START_TIME.HOUR;
            lastYearMon = Math.max(lastYearMon, row.START_TIME.yearMon());

            if (row.VALUE <= 0.001) {
                zerroCnt++;
            }

            cellSet.add(row.CELL_LAC_ID);
            daySet.add(row.START_TIME.getDate());

            StatCell statCell = cellMap.get(row.CELL_LAC_ID);
            if (statCell == null) {
                statCell = new StatCell(row.CELL_LAC_ID);
                cellMap.put(row.CELL_LAC_ID, statCell);
            }
            statCell.update(row);
        }

        public static void writeHeader(StringBuilder sb, String prefix) {
            sb.append(";" + prefix + "SESZ");
            sb.append(";" + prefix + "VAL_AVGS");
        }

        public void writeValues(StringBuilder sb, StatUser stat2) {
            double zerroCnt = Utils.div(this.zerroCnt, this.sessionCnt);
            double zerroCnt2 = Utils.div(stat2.zerroCnt, stat2.sessionCnt);
            sb.append(";" + Utils.numDiv(zerroCnt, zerroCnt2));

            double totalValue1 = Utils.div(this.totalValue, this.sessionCnt);
            double totalValue2 = Utils.div(stat2.totalValue, stat2.sessionCnt);
            sb.append(";" + Utils.numDiv(totalValue1, totalValue2));
        }
    }

    public void loadData(String fileName) throws Exception {
        Set<Integer> cellSet = new HashSet<>();
        Set<Integer> skSet = new HashSet<>();
        log.warn("Loading file: {}...", fileName);

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line = br.readLine();

            while ((line = br.readLine()) != null) {
                DataRow row = new DataRow(line);
                cellSet.add(row.CELL_LAC_ID);
                skSet.add(row.SK_ID);
                list.add(row);

                if (list.size() % 200_000 == 0) {
                    log.warn("Loading file {}, {} rows...", fileName, list.size());
                }

                if (list.size() >= maxRows) {
                    break;
                }
            }
        }

        log.warn("Loading file {}, {} rows, SK_ID={}, CELL_LAC_ID={} ...", fileName, list.size(), skSet.size(), cellSet.size());
    }

    public class DataRow {
        public int SK_ID; // Уникальный идентификатор абонента
        public int CELL_LAC_ID; // Уникальный идентификатор соты
        public double VALUE; // Количество потребленного Data трафика за одну сессию (Мб)
        public DateExt START_TIME; // Время начала сессии

        // SK_ID;CELL_LAC_ID;DATA_VOL_MB;START_TIME
        // 4147;252825;,000012788928813859606893354079490718725;29.05 17:27:35

        //SK_ID;CELL_LAC_ID;VOICE_DUR_MIN;START_TIME
        //373;269094;,0015972208333333333;27.04 23:00:00

        public DataRow(String line) {
            List<String> data = Lists.newArrayList(Splitter.on(";").split(line));

            SK_ID = Integer.parseInt(data.get(0));
            CELL_LAC_ID = Integer.parseInt(data.get(1));
            VALUE = Utils.getDouble(data.get(2));
            START_TIME = new DateExt(data.get(3));
        }
    }
}
