package mail.csi.predict;

import mail.csi.ValueAgg;

public class ResultStat {
    ValueAgg agg = new ValueAgg();
    double value = 0;
    int skId;
    int sci;

    int counter0 = 0;
    int counter1 = 0;

    public double votePct1() {
        double div = counter0 + counter1;
        if (div == 0) {
            return 0.5d;
        }

        return counter1 / div;
    }

    public ResultStat(int skId, int sci) {
        this.skId = skId;
        this.sci = sci;
    }

    public void update(int valueSki, double valueProb) {
        if (sci != valueSki) {
            throw new RuntimeException("err");
        }
        agg.update(valueProb);

        if (valueProb < 0.5) {
            counter0++;
        }
        else {
            counter1++;
        }
    }
}
