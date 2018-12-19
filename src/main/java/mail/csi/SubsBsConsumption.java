package mail.csi;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * Created by demo on 5/11/2018.
 */
public class SubsBsConsumption {
    private static final Logger log = LoggerFactory.getLogger(SubsBsConsumption.class);
    public HashMap<Integer, HashMap<Integer, DataStat>> userMonAvg = new HashMap<>();
    public HashMap<Integer, HashMap<Integer, DataStat>> userCellAvg = new HashMap<>();

    private DataStat totalAvg = new DataStat(0);
    public HashMap<Integer, DataStat> userAvg = new HashMap<>();
    private int monthExport = 0;

    public SubsBsConsumption(int monthExport) {
        this.monthExport = monthExport;
    }

    public String headers() {
        StringBuilder sb = new StringBuilder();
        sb.append(";DMON;VMON");

        for (int month = 0; month < monthExport; month++) {
            sb.append(";MON_MINUTES" + month);
            sb.append(";MON_DATA_MB" + month);
            sb.append(";MON_DATA_MIN" + month);
            sb.append(";MON_DATA_SPD" + month);
        }

        sb.append(headerGroup("A_"));
        sb.append(headerGroup("B_"));
        sb.append(headerGroup("C_"));
        sb.append(headerGroup("D_"));
        sb.append(headerGroup("E_"));
        return sb.toString();
    }

    private String headerGroup(String prefix) {
        StringBuilder sb = new StringBuilder();

        sb.append(";BASE_MINUTES;PCT_MINUTES");
        sb.append(";BASE_DATA_MIN;PCT_DATA_MIN");
        sb.append(";BASE_DATA_MB;PCT_DATA_MB");
        sb.append(";BASE_DATA_SPD;PCT_DATA_SPD");

        String header = sb.toString().replace(";", ";" + prefix);
        return header;
    }

    public String values(RowSci row) {
        HashMap<Integer, DataStat> monMap = userMonAvg.get(row.SK_ID);
        HashMap<Integer, DataStat> cellMap = userCellAvg.get(row.SK_ID);

        if (monMap == null || cellMap == null) {
            return null;
        }

        ArrayList<DataStat> userCells = new ArrayList<>(cellMap.values());
        if (userCells.size() == 0) {
            throw new RuntimeException("list.size() == 0");
        }

        DataStat userAvg = this.userAvg.get(row.SK_ID);
        if (userAvg == null) {
            throw new RuntimeException("userAvg == null");
        }

        int YEAR_MON_CSI = row.CONTACT_DATE.yearMon();
        DataStat statLast = monMap.get(YEAR_MON_CSI - 1);
        DataStat statPrev = monMap.get(YEAR_MON_CSI - 2);

        if (statLast == null) {
            statLast = statPrev;
        }
        if (statPrev == null) {
            statPrev = statLast;
        }

        if (statLast == null) {
            statLast = userAvg;
        }
        if (statPrev == null) {
            statPrev = userAvg;
        }

        int dataMon = 0;
        int voiceMon = 0;
        for (DataStat stat : monMap.values()) {
            if (stat.YEAR_MON >= YEAR_MON_CSI) {
                continue;
            }

            if (stat.SUM_DATA_MIN.avg() > 0) {
                dataMon++;
            }

            if (stat.SUM_MINUTES.avg() > 0) {
                voiceMon++;
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("" + dataMon + ";" + voiceMon);

        for (int month = 0; month < monthExport; month++) {
            DataStat stat = monMap.get(YEAR_MON_CSI - month);
            if (stat == null) {
                stat = userAvg;
            }

            sb.append(";" + Utils.num(stat.SUM_MINUTES.avg()));
            sb.append(";" + Utils.num(stat.SUM_DATA_MB.avg()));
            sb.append(";" + Utils.num(stat.SUM_DATA_MIN.avg()));

            double speed = Utils.div(stat.SUM_DATA_MB.avg(), stat.SUM_DATA_MIN.avg());
            sb.append(";" + Utils.num(speed));
        }

        appendDelta(sb, statPrev, statLast);
        appendDelta(sb, userAvg, statLast);
        appendDelta(sb, totalAvg, statLast);

        userCells.sort((t1, t2) -> Double.compare(t2.SUM_MINUTES.getTotal(), t1.SUM_MINUTES.getTotal()));
        DataStat voiceMax = userCells.get(0);
        appendDelta(sb, voiceMax, statLast);


        userCells.sort((t1, t2) -> Double.compare(t2.SUM_DATA_MIN.getTotal(), t1.SUM_DATA_MIN.getTotal()));
        DataStat dataMax = userCells.get(0);
        appendDelta(sb, dataMax, statLast);

        return sb.toString();
    }

    public void appendDelta(StringBuilder sb, DataStat base, DataStat stat) {
        sb.append(";" + Utils.num(base.SUM_MINUTES.avg()));
        sb.append(";" + Utils.numDiv(base.SUM_MINUTES.avg(), stat.SUM_MINUTES.avg()));

        sb.append(";" + Utils.num(base.SUM_DATA_MIN.avg()));
        sb.append(";" + Utils.numDiv(base.SUM_DATA_MIN.avg(), stat.SUM_DATA_MIN.avg()));

        sb.append(";" + Utils.num(base.SUM_DATA_MB.avg()));
        sb.append(";" + Utils.numDiv(base.SUM_DATA_MB.avg(), stat.SUM_DATA_MB.avg()));

        double speed1 = Utils.div(base.SUM_DATA_MB.avg(), base.SUM_DATA_MIN.avg());
        double speed2 = Utils.div(stat.SUM_DATA_MB.avg(), stat.SUM_DATA_MIN.avg());

        sb.append(";" + Utils.num(speed1));
        sb.append(";" + Utils.numDiv(speed1, speed2));
    }

    public void loadData(String fileName) throws Exception {
        log.warn("Loading file: {}...", fileName);

        int counter = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line = br.readLine();

            while ((line = br.readLine()) != null) {
                DataRow row = new DataRow(line);
                totalAvg.update(row);

                {
                    HashMap<Integer, DataStat> userMon = userMonAvg.get(row.SK_ID);

                    if (userMon == null) {
                        userMon = new HashMap<>();
                        userMonAvg.put(row.SK_ID, userMon);
                    }

                    DataStat stat = userMon.get(row.YEAR_MON);
                    if (stat == null) {
                        stat = new DataStat(row.YEAR_MON);
                        userMon.put(row.YEAR_MON, stat);
                    }
                    stat.update(row);
                }

                {
                    HashMap<Integer, DataStat> userCell = userCellAvg.get(row.SK_ID);

                    if (userCell == null) {
                        userCell = new HashMap<>();
                        userCellAvg.put(row.SK_ID, userCell);
                    }

                    DataStat stat = userCell.get(row.CELL_LAC_ID);
                    if (stat == null) {
                        stat = new DataStat(0);
                        userCell.put(row.CELL_LAC_ID, stat);
                    }
                    stat.update(row);
                }

                {
                    DataStat stat = userAvg.get(row.SK_ID);
                    if (stat == null) {
                        stat = new DataStat(0);
                        userAvg.put(row.SK_ID, stat);
                    }
                    stat.update(row);
                }

                if (++counter % 50_000 == 0) {
                    log.warn("Loading file: {}, {} rows...", fileName, counter);
                }
            }
        }

        log.warn("Loading file {}, {} rows...", fileName, counter);
    }

    public class DataStat {
        private int YEAR_MON;

        private ValueAgg SUM_MINUTES = new ValueAgg();
        private ValueAgg SUM_DATA_MB = new ValueAgg();
        private ValueAgg SUM_DATA_MIN = new ValueAgg();

        public DataStat(int YEAR_MON) {
            this.YEAR_MON = YEAR_MON;
        }

        public void update(DataRow row) {
            this.SUM_MINUTES.update(row.SUM_MINUTES);
            this.SUM_DATA_MB.update(row.SUM_DATA_MB);
            this.SUM_DATA_MIN.update(row.SUM_DATA_MIN);
        }
    }

    public class DataRow {
        public int SK_ID; // Уникальный идентификатор абонента
        public int CELL_LAC_ID; // Уникальный идентификатор соты
        public int YEAR_MON; // Месяц расчета показателей

        public double SUM_MINUTES; // Суммарный голосовой трафик опрашиваемого абонента на этой соте
        public double SUM_DATA_MB; // Суммарный data- трафик опрашиваемого абонента на этой соте
        public double SUM_DATA_MIN; // Суммарная продолжительность data-сессий этого абонента на этой соте

        // SK_ID;CELL_LAC_ID;MON;SUM_MINUTES;SUM_DATA_MB;SUM_DATA_MIN
        //1973;343146;01.05;;,000043612473803066340435135238939101372;,000118985095213063040445175594633962582

        public DataRow(String line) {
            List<String> data = Lists.newArrayList(Splitter.on(";").split(line));

            SK_ID = Integer.parseInt(data.get(0));
            CELL_LAC_ID = Integer.parseInt(data.get(1));

            DateExt dateMon = new DateExt(data.get(2));
            if (dateMon.DAY != 1) {
                throw new RuntimeException("Bad value DAY != 1: " + data.get(2));
            }
            YEAR_MON = dateMon.yearMon();

            SUM_MINUTES = Utils.getDouble(data.get(3));
            SUM_DATA_MB = Utils.getDouble(data.get(4));
            SUM_DATA_MIN = Utils.getDouble(data.get(5));
        }
    }
}
