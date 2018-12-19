package mail.csi.predict;

import mail.csi.Utils;
import mail.csi.dataset.DataList;
import mail.csi.dataset.DataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class PredictScore {
    private static final Logger log = LoggerFactory.getLogger(PredictScore.class);

    private List<ResultRow> sourceList = new ArrayList<>();
    public HashMap<String, PredictStat> statMap = new HashMap<>();
    public HashMap<String, Double> bestKof = new HashMap<>();
    public HashMap<Integer, Integer> voteMap = new HashMap<>();

    private String filePath;

    public PredictScore(String filePath, boolean hasSci) throws Exception {
        DataList data = new DataList();
        this.filePath = filePath;
        data.load(filePath);

        for (DataRow row : data.rows()) {
            ResultRow row2 = new ResultRow();
            row2.SK_ID = row.getInt("SK_ID");

            if (hasSci) {
                row2.CSI_ORIG = row.getInt("CSI");
            }
            sourceList.add(row2);
        }
    }

    public void save(double score) throws IOException {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmm");
        String formatDateTime = now.format(formatter);

        save("submit-" + formatDateTime + "-" + (int) (10000 * score) + ".csv");
    }

    public void save(String fileName) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            int counter = 0;
            for (ResultRow row : sourceList) {
                writer.write("" + row.CSI_PRED);
                writer.write(System.lineSeparator());
            }
        }
    }

    public void update(String code, DataList data) {
        if (code.endsWith(".csv")) {
            throw new IllegalArgumentException("Bad code: " + code);
        }

        PredictStat stat = statMap.get(code);
        if (stat == null) {
            stat = new PredictStat(code);
            statMap.put(code, stat);
            bestKof.put(code, 1.0d);
        }

        for (DataRow row : data.rows()) {
            int userId = row.getInt("SK_ID");
            Integer voteCnt = voteMap.get(userId);

            if (voteCnt == null) {
                voteMap.put(userId, 1);
            } else {
                voteMap.put(userId, voteCnt + 1);
            }
        }

        stat.update(data);
    }

    public void selectBest() {
        double bestScore = 0d;
        Random rand = new Random();

        if (bestKof.size() == 0) {
            throw new IllegalArgumentException("Bad bestKof.size()");
        }

        for (String code : bestKof.keySet()) {
            bestKof.put(code, 1.0d);
        }
        bestKof.put("std", 0.1d);

        int totalNum = 2000;
        HashMap<String, Double> kofNew = new HashMap<>();

        for (int ind = 1; ind <= totalNum; ind++) {
            kofNew.clear();
            for (String code : bestKof.keySet()) {
                double dx = 0.04 - 0.08 * rand.nextDouble();
                double kof = bestKof.get(code);
                kofNew.put(code, kof + dx);
            }

            double score = predict(kofNew);

            if (bestScore < score) {
                bestKof = new HashMap<>(kofNew);
                bestScore = score;
            }

            if (ind % 500 == 0 || ind == totalNum) {
                log.warn("Best {}/{}: score={}, bestScore={}", ind, totalNum, score, bestScore);
            }
        }

        StringBuilder sb = new StringBuilder();
        for (String code : bestKof.keySet()) {
            sb.append("\n\t" + code + "=" + Utils.num2(bestKof.get(code)));
        }
        log.warn("BestKof: {}", sb.toString());
    }

    public double predict(HashMap<String, Double> kofMap) {
        double levelStd = kofMap.get("std");

        for (ResultRow row : sourceList) {
            double total = 0;
            int totalCnt = 0;
            for (Map.Entry<String, PredictStat> pair : statMap.entrySet()) {
                Double kof = kofMap.get(pair.getKey());
                if (kof == null) {
                    throw new IllegalArgumentException("Key is not found: " + pair.getKey());
                }

                ResultStat rowStat = pair.getValue().mapData.get(row.SK_ID);
                if (rowStat != null) {
                    double std = rowStat.agg.std();
                    if (std < levelStd) {
                        totalCnt++;
                        //total += kof * rowStat.agg.avg2();
                        total += kof * rowStat.votePct1();
                    }
                }
            }

            if (totalCnt == 0) {
                row.CSI_PROB = 0;
                row.CSI_PRED = 0;
            } else {
                row.CSI_PROB = (total / totalCnt);
                row.CSI_PRED = (row.CSI_PROB > 0.5d) ? 1 : 0;
            }
        }

        double score = scoreAuc2();
        return score;
    }

    public double scoreAuc2() {
        ScoreAuc metric = new ScoreAuc();
        for (ResultRow row : sourceList) {
            Integer voteCnt = voteMap.get(row.SK_ID);

            if (voteCnt != null) {
                metric.update(row.CSI_ORIG, row.CSI_PRED);
            }
        }

        double score = metric.score();
        return score;
    }

    public void printScore() {
        ScoreAuc metric = new ScoreAuc();
        for (ResultRow row : sourceList) {
            Integer voteCnt = voteMap.get(row.SK_ID);

            if (voteCnt != null) {
                metric.update(row.CSI_ORIG, row.CSI_PRED);
            }
        }

        log.warn("\nPerformance result sourceFile: {}, size {}/{}", filePath, voteMap.size(), sourceList.size());
        metric.printScore(log);
    }

    public static class ResultRow {
        int SK_ID;
        int CSI_ORIG;
        int CSI_PRED;
        double CSI_PROB;
    }
}
