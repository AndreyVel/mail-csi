package mail.csi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by demo on 2/20/2018.
 */
public class ValueAgg {
    private int counter = 0;
    private double total;
    private double total2;
    private double valueMin = Double.MAX_VALUE;
    private double valueMax = Double.MIN_VALUE;

    private double uniqueDiv = 10000;
    private HashMap<Integer, Integer> uniqueMap = new HashMap<>();
    private List<Double> values = new ArrayList<>();

    @Override
    public String toString() {
        return avg() + ", #" + counter;
    }

    public void update(Double value) {
        if (value == null || value == 0) {
            return;
        }

        int valueNum = (int) Math.round(value * uniqueDiv);
        Integer uniqCnt = uniqueMap.get(valueNum);
        if (uniqCnt == null) {
            uniqueMap.put(valueNum, 1);
        } else {
            uniqueMap.put(valueNum, uniqCnt + 1);
        }

        counter++;
        total += value;
        total2 += value * value;
        valueMin = Math.min(valueMin, value);
        valueMax = Math.max(valueMax, value);
        values.add(value);
    }

    public double getTotal() {
        return total;
    }

    public Double min() {
        if (counter == 0) {
            return null;
        }

        return valueMin;
    }

    public Double max() {
        if (counter == 0) {
            return null;
        }

        return valueMax;
    }

    public int counter() {
        return counter;
    }

    public double avg() {
        if (counter == 0) {
            return 0;
        }

        return total / counter;
    }

    public double uniqueNum() {
        return uniqueMap.size();
    }

    public double uniqueMod() {
        int keyMax = -1;
        int valueMax = -1;

        for (int key : uniqueMap.keySet()) {
            int value = uniqueMap.get(key);
            if (value > valueMax) {
                valueMax = value;
                keyMax = key;
            }
        }
        return (double) keyMax / uniqueDiv;
    }

    public double avg2() {
        if (counter == 0) {
            return 0;
        }

        return Math.sqrt(total2 / counter);
    }

    public double std() {
        if (counter == 0) {
            return 0;
        }

        if (counter != values.size()) {
            throw new RuntimeException("err");
        }

        double avg = avg();
        double total = 0;

        for (Double value : values) {
            double delta = avg - value;
            total += delta * delta;
        }
        return Math.sqrt(total / counter);
    }
}
