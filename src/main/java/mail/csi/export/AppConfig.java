package mail.csi.export;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by demo on 3/8/2018.
 */
public class AppConfig {
    private  static String CONFIG_FILE = "app.config";
    private static Properties prop = null;

    private static synchronized String getProperty(String propertyKey) {
        try {
            if (prop == null) {
                FileInputStream input = new FileInputStream(CONFIG_FILE);
                prop = new Properties();
                prop.load(input);
            }

            String value = prop.getProperty(propertyKey);
            if (value == null) {
                throw new RuntimeException("Property value is not found by key: " + propertyKey);
            }
            return value;
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static String getDataPathIn() {
        return getProperty("data_path_in");
    }

    private static String getDataPathOut() {
        return getProperty("data_path_out");
    }

    public static String getPredictPath() {
        return getProperty("predict_path");
    }

    public static String getFileNameIn(String dataTag, String fileName) {
        String filePath = getDataPathIn();

        if (fileName.startsWith("subs")) {
            filePath = Paths.get(filePath, dataTag).toString();

            filePath = Paths.get(filePath, fileName + "_" + dataTag).toString();
            return filePath + ".csv";
        }

        filePath = Paths.get(filePath, fileName).toString();
        return filePath + ".csv";
    }

    public static String getFileNameOut(String dataTag, String fileName) {
        Path exportPath = Paths.get(getDataPathOut(), dataTag);

        if (!Files.exists(exportPath)) {
            try {
                Files.createDirectory(exportPath);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        String filePath = Paths.get(exportPath.toString(), fileName).toString();
        return filePath + ".csv";
    }

}
