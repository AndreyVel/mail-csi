package mail.csi.predict;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

public class ScoreAuc {
    public int tp = 0;
    public int fn = 0;

    public int fp = 0;
    public int tn = 0;

    public void update(int value, int valuePred) {
        if (value == 1) {
            if (valuePred == 1) {
                tp++;
            } else {
                fn++;
            }
        }

        if (value == 0) {
            if (valuePred == 1) {
                fp++;
            } else {
                tn++;
            }
        }
    }

    public void update(int value, double valueProb, double threshold) {
        if (value == 1) {
            if (valueProb > threshold) {
                tp++;
            } else {
                fn++;
            }
        }

        if (value == 0) {
            if (valueProb > threshold) {
                fp++;
            } else {
                tn++;
            }
        }
    }

    public void printScore(Logger log) {
        log.warn("---------- Predict ----------");
        log.warn("0 {} {}", StringUtils.leftPad("" + this.tn, 8), StringUtils.leftPad("" + this.fp, 8));
        log.warn("1 {} {}", StringUtils.leftPad("" + this.fn, 8), StringUtils.leftPad("" + this.tp, 8));
        log.warn("ScoreAuc={}, total={}", this.score(), tn + fp + fn + tp);
    }

    public double score() {
        double recall = (double) tp / (tp + fn);
        double specificity = (double) tn / (fp + tn);

        double score = (recall + specificity) / 2;
        return score;
    }
}

/*
------Actual---
-------1--0----
pred-1 TP|FP
pred-0 FN|TN

AUC = (recall + specificity) / 2

i.e. you need to calculate the 'specificity': specificity = tn / (fp + tn)
In other words you need to know at least tn, fp and recall ( = tp / (tp + fn)).

----------------------------------------------------------------------
FP = confusion_matrix.sum(axis=0) - np.diag(confusion_matrix)
FN = confusion_matrix.sum(axis=1) - np.diag(confusion_matrix)
TP = np.diag(confusion_matrix)
TN = confusion_matrix.values.sum() - (FP + FN + TP)

# Sensitivity, hit rate, recall, or true positive rate
TPR = TP/(TP+FN)
# Specificity or true negative rate
TNR = TN/(TN+FP)
# Precision or positive predictive value
PPV = TP/(TP+FP)
# Negative predictive value
NPV = TN/(TN+FN)
# Fall out or false positive rate
FPR = FP/(FP+TN)
# False negative rate
FNR = FN/(TP+FN)
# False discovery rate
FDR = FP/(TP+FP)

# Overall accuracy
ACC = (TP+TN)/(TP+FP+FN+TN)*/
