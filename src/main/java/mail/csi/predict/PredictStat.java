package mail.csi.predict;

import mail.csi.dataset.DataList;
import mail.csi.dataset.DataRow;
import java.util.HashMap;

public class PredictStat {
    public String code;
    public HashMap<Integer, ResultStat> mapData = new HashMap<>();

    public PredictStat(String tag) {
        this.code = tag;
    }

    public void update(DataList data) {
        for (DataRow row : data.rows()) {
            int sci = row.getInt("CSI");
            double prob = row.getDouble("PROB");

            int userId = row.getInt("SK_ID");
            ResultStat stat = mapData.get(userId);
            if (stat == null) {
                stat = new ResultStat(userId, sci);
                mapData.put(userId, stat);
            }

            stat.update(sci, prob);
        }
    }
}