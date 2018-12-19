package mail.csi.export;

import mail.csi.*;
import mail.csi.dataset.*;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;

/**
 * Created by demo on 5/10/2018.
 */
public class AppExport {
    private static final Logger log = LoggerFactory.getLogger(AppExport.class);

    public static void main(String[] arg) throws Exception {
        BasicConfigurator.configure();

        String[] dataTags = new String[]{"train", "test"};
        for (String dataTag : dataTags) {
            UserSci mapSci = new UserSci();
            mapSci.loadData(AppConfig.getFileNameIn(dataTag,"subs_csi"));

            if (1 == 1) {
                DataList featureMon = new DataList();
                featureMon.load(AppConfig.getFileNameIn(dataTag,"subs_features"));
                ExportFeatures.execute(AppConfig.getFileNameOut(dataTag,"features"), mapSci, featureMon);
            }

            if (1 == 1) {
                SubsBsSession sessionVoice = new SubsBsSession();
                sessionVoice.loadData(AppConfig.getFileNameIn(dataTag,"subs_bs_voice_session"));
                sessionVoice.exportSession(AppConfig.getFileNameOut(dataTag,"voice_session"), mapSci, "VOICE_");

                SubsBsSession sessionData = new SubsBsSession();
                sessionData.loadData(AppConfig.getFileNameIn(dataTag,"subs_bs_data_session"));
                sessionData.exportSession(AppConfig.getFileNameOut(dataTag,"data_session"), mapSci, "DATA_");
            }

            if (1 == 1) {
                SubsBsConsumption cons = new SubsBsConsumption(3);
                cons.loadData(AppConfig.getFileNameIn(dataTag,"subs_bs_consumption"));

                exportConsumption(AppConfig.getFileNameOut(dataTag,"consumption"), mapSci, cons);
            }
        }
    }

    private static void exportConsumption(String filePath, UserSci sci, SubsBsConsumption cons) throws Exception {
        log.warn("Export file: {}", filePath);

        int countSkip = 0;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write("CSI;SK_ID");
            writer.write(cons.headers());
            writer.write(System.lineSeparator());

            for (RowSci row : sci.rows()) {
                String values = cons.values(row);

                if (values == null) {
                    log.warn("No Consumption: SK_ID={}, countSkip={}", row.SK_ID, countSkip++);
                    continue;
                }

                writer.write(row.CSI + ";");
                writer.write(row.SK_ID + ";");

                writer.write(values);
                writer.write(System.lineSeparator());
            }
        }
    }
}
