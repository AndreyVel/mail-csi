package mail.csi.export;

import mail.csi.RowSci;
import mail.csi.dataset.DataList;
import mail.csi.dataset.DataRow;
import mail.csi.UserSci;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;

public class AppBinMap {
    private static final Logger log = LoggerFactory.getLogger(AppExport.class);
    public static int SESSION_TOP_CELLS = 2000;
    public static int BIN_NUMBER = 16;

    public static void main(String[] arg) throws Exception {
        BasicConfigurator.configure();

        String[] dataTags = new String[]{"train", "test"};
        for (String dataTag : dataTags) {
            UserSci mapSci = new UserSci();
            mapSci.loadData(AppConfig.getFileNameIn(dataTag, "subs_csi"));

            if (1 == 1) {
                DataList listSes = new DataList();
                listSes.load(AppConfig.getFileNameIn(dataTag, "subs_bs_data_session"));
                exportData(AppConfig.getFileNameOut(dataTag, "data_pca"), mapSci, listSes, "DATA_VOL_MB");
            }

            if (1 == 1) {
                DataList listSes = new DataList();
                listSes.load(AppConfig.getFileNameIn(dataTag, "subs_bs_voice_session"));
                exportData(AppConfig.getFileNameOut(dataTag, "voice_pca"), mapSci, listSes, "VOICE_DUR_MIN");
            }
        }

        log.warn("The end...");
    }

    public static void exportData(String filePath, UserSci sci, DataList data, String dataField) throws IOException {
        HashMap<Integer, int[]> binMap = new HashMap<>();
        HashMap<Integer, Integer> cellMap = new HashMap<>();

        for (DataRow row : data.rows()) {
            int cellId = row.getInt("CELL_LAC_ID");
            cellMap.put(cellId, cellMap.size());
        }
        log.warn("Export file {}, {} rows", filePath, cellMap.size());

        HashSet<Integer> notFoundUserInData = new HashSet<>();
        HashSet<Integer> notFoundDataInUser = new HashSet<>();

        for (DataRow row : data.rows()) {
            int userId = row.getInt("SK_ID");
            RowSci userRow = sci.map.get(userId);
            if (userRow == null) {
                notFoundUserInData.add(userId);
                continue;
            }
            LocalDate dateOpr = userRow.CONTACT_DATE.getDate();

            LocalDate dateSes = row.getDate("START_TIME");
            if (dateOpr.isBefore(dateSes)) {
                throw new RuntimeException("errr");
            }

            int[] cellBin = binMap.get(userId);
            if (cellBin == null) {
                cellBin = new int[SESSION_TOP_CELLS];
                binMap.put(userId, cellBin);
            }

            int cellId = row.getInt("CELL_LAC_ID");
            for (int ind = 0; ind < BIN_NUMBER; ind++) {
                int binInd = (cellId * (31 + 7 * ind) % SESSION_TOP_CELLS);
                cellBin[binInd] = cellBin[binInd] + 1;
            }
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write("CSI;SK_ID");
            for (int ind = 0; ind < SESSION_TOP_CELLS; ind++) {
                writer.write(";" + "B" + ind);
            }
            writer.write(System.lineSeparator());

            for (RowSci row : sci.rows()) {
                int[] cellBin = binMap.get(row.SK_ID);
                if (cellBin == null) {
                    notFoundDataInUser.add(row.SK_ID);
                    continue;
                }

                writer.write(row.CSI + ";" + row.SK_ID);

                for (int ind = 0; ind < SESSION_TOP_CELLS; ind++) {
                    writer.write(";" + cellBin[ind]);
                }
                writer.write(System.lineSeparator());
            }

        }

        log.warn("notFoundUserInData={} / {}", notFoundUserInData.size(), sci.rows().size());
        log.warn("notFoundDataInUser={} / {}", notFoundDataInUser.size(), data.rows().size());
    }
}
