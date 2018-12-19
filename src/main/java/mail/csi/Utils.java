package mail.csi;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.Locale;

/**
 * Created by demo on 5/11/2018.
 */
public class Utils {
    private static NumberFormat num2 = new DecimalFormat("#0.00", DecimalFormatSymbols.getInstance(Locale.US));
    private static DecimalFormat decimalFormat = new DecimalFormat("0.#");
    private static DecimalFormat num9 = new DecimalFormat("0.000000000");

    {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols();
        symbols.setDecimalSeparator('.');
        decimalFormat.setDecimalFormatSymbols(symbols);
    }

    public static boolean isZerro(double val) {
        return Math.abs(val) < 0.000001d;
    }

    public static int yearMon(LocalDate date) {
        return date.getYear() * 100 + date.getMonthValue();
    }

    public static void assertDouble(double val) {
        if (val == Double.NaN) {
            throw new RuntimeException("NaN");
        }
        if (val == Double.POSITIVE_INFINITY) {
            throw new RuntimeException("POSITIVE_INFINITY");
        }
        if (val == Double.NEGATIVE_INFINITY) {
            throw new RuntimeException("NEGATIVE_INFINITY");
        }
    }

    public static String num2(double val) {
        assertDouble(val);
        return num2.format(val);
    }

    public static double div(double val, double div) {
        if (Math.abs(div) < 0.000000001d) {
            return 0d;
        }

        return val / div;
    }

    public static String numDiv(double val, double div) {
        if (div == 0) {
            return num(0);
        }

        return num(val / div);
    }

    public static String logDiv(double val, double div) {
        if (div == 0) {
            return num(0);
        }

        double value = val / div;
        value = Math.log(value);
        return num(value);
    }

    public static String num(double val) {
        assertDouble(val);
        return num9.format(val);
    }

    public static double getDouble(String value) {
        if (value == null || value.isEmpty()) {
            return 0;
        }

        double res = 0;
        value = value.replace(",", ".");

        try {
            res = decimalFormat.parse(value).doubleValue();
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }
        return res;
    }

    public static boolean between(int value, int valueBeg, int valueEnd) {
        return valueBeg <= value && value <= valueEnd;
    }

    public static boolean lte(LocalDate date1, LocalDate date2) {
        return !date1.isAfter(date2);
    }

    public static boolean gte(LocalDate date1, LocalDate date2) {
        return !date2.isAfter(date1);
    }

    public static boolean between(LocalDate date, LocalDate from, LocalDate to) {
        return lte(from, date) && lte(date, to);
    }

    public static int getInt(String value) {
        if (value == null || value.isEmpty()) {
            return 0;
        }

        int res = Integer.parseInt(value);
        return res;
    }
}
