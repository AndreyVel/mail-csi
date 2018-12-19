package mail.csi.predict;

import mail.csi.dataset.*;
import mail.csi.export.AppConfig;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by demo on 5/10/2018.
 */
public class AppSubmit {
    private static final Logger log = LoggerFactory.getLogger(AppSubmit.class);

    public static void main(String[] arg) throws Exception {
        BasicConfigurator.configure();
        PredictScore resTrain = new PredictScore( AppConfig.getFileNameIn("train", "subs_csi"), true);
        PredictScore resSubmit = new PredictScore(AppConfig.getFileNameIn("test", "subs_csi"), false);

        String predictPath = AppConfig.getPredictPath();
        File[] files = (new File(predictPath)).listFiles();

        for (File file : files) {
            if (file.isDirectory()) continue;
            String fileName = file.getName();

            DataList data = new DataList();
            data.load(file.getCanonicalPath());

            if (fileName.contains("-t-")) {
                String[] tags = fileName.split("-t-");
                resTrain.update(tags[0], data);
            } else if (fileName.contains("-s")) {
                String[] tags = fileName.split("-s-");
                resSubmit.update(tags[0], data);
            } else {
                throw new RuntimeException("err");
            }
        }

        showAnomalyProp(resTrain, 0, 50);
        showAnomalyProp(resTrain, 1, 20);

        resTrain.selectBest();
        double scoreTrain = resTrain.predict(resTrain.bestKof);
        double scoreSubmit = resSubmit.predict(resTrain.bestKof);

        resTrain.printScore();
        resSubmit.printScore();

        resSubmit.save(scoreTrain);
    }

    public static void showAnomalyProp(PredictScore resTrain, int valueSci, int limit) {
        List<ResultStat> listSk = new ArrayList<>();
        for (PredictStat pred : resTrain.statMap.values()) {
            for (ResultStat stat : pred.mapData.values()) {
                if (stat.sci == valueSci) {
                    stat.value = stat.agg.avg();
                    listSk.add(stat);
                }
            }
        }

        if (valueSci == 0) {
            listSk.sort((t0, t1) -> Double.compare(t1.value, t0.value));
        }
        if (valueSci == 1) {
            listSk.sort((t0, t1) -> Double.compare(t0.value, t1.value));
        }

        StringBuilder sb = new StringBuilder();
        log.warn("=====================================================");

        int counter = 0;
        for (ResultStat stat : listSk) {
            log.warn("skId={}, sci={}, counter={}, agg={}", stat.skId, stat.sci, stat.agg.counter(), stat.agg.avg());

            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(stat.skId);

            if (++counter >= limit) {
                break;
            }
        }

        log.warn("Anomaly SK_ID: mapSize={}, sci={}: {}", resTrain.voteMap.size(), valueSci, sb.toString());
    }
}
