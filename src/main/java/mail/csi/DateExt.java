package mail.csi;

import java.time.LocalDate;

import static java.time.temporal.ChronoUnit.DAYS;

/**
 * Created by demo on 5/11/2018.
 */
public class DateExt {
    public int DAY;
    public int MON;
    public int YEAR = 2002;

    public int HOUR;
    //29.05 17:27:35
    private LocalDate dateCache = null;

    public DateExt(String value) {
        String[] data = value.split(" ");
        String[] data2 = data[0].split("\\.");

        if (data2.length == 2) {
            DAY = Integer.parseInt(data2[0]);
            MON = Integer.parseInt(data2[1]);
        }
        else if (data2.length == 3) {
            DAY = Integer.parseInt(data2[0]);
            MON = Integer.parseInt(data2[1]);

            YEAR = 2000 + Integer.parseInt(data2[2]);
        }
        else {
            throw new RuntimeException("Bad date: " + value);
        }

        if (data.length == 2) {
            String[] time = data[1].split(":");;
            if (time.length > 2) {
                HOUR = Integer.parseInt(time[0]);
            }
        }
        else if (data.length > 2){
            throw new RuntimeException("Bad date: " + value);
        }

        if (YEAR != 0 && !Utils.between(YEAR, 2001, 2002)) {
            throw new RuntimeException("Bad YEAR: " + value);
        }
    }

    public LocalDate getDate() {
        if (dateCache == null) {
            dateCache = LocalDate.of(YEAR, MON, DAY);
        }
        return dateCache;
    }

    public int rangeDays(DateExt dat) {
        LocalDate data0 = this.getDate();
        LocalDate data1 = dat.getDate();

        long delta = DAYS.between(data0, data1);
        return (int)delta + 1;
    }

    public int yearMon() {
        return YEAR * 100 + MON;
    }

    public String getDateKey() {
        String dateKey = String.format("%02d-%02d-%02d", YEAR, MON, DAY);
        return dateKey;
    }

    @Override
    public String toString() {
        String dateKey = String.format("%02d-%02d-%02d %02d", YEAR, MON, DAY, HOUR);
        return dateKey;
    }
}
