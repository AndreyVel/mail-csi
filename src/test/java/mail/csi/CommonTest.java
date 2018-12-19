package mail.csi;

import mail.csi.dataset.DataList;
import mail.csi.dataset.DataRow;
import mail.csi.predict.PredictScore;
import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.time.LocalDate;

public class CommonTest {
    private static final String RESOURCE_PATH = "./src/test/resources/";

    @Before
    public void beforeTest() {
        BasicConfigurator.configure();
    }

    @Test
    public void testAgg() throws Exception {
        File tempFile = File.createTempFile("prefix-", "-suffix");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            writer.write("ID;SK_ID;IS_NEW\n");

            writer.write("4;2;2\n");
            writer.write("8;3;2\n");
            writer.write("2;3;3\n");
            writer.write("2;3;3\n");
        }

        DataList data = new DataList();
        data.load(tempFile.getCanonicalPath());

        ValueAgg agg = new ValueAgg();
        for (DataRow row : data.rows()) {
            agg.update(row.getDouble("ID"));
        }

        Assert.assertEquals(agg.uniqueMod(), 2, 0.001);
        Assert.assertEquals(agg.uniqueNum(), 3, 0.001);
        Assert.assertEquals(agg.avg(), 4, 0.001);
        Assert.assertEquals(agg.min(), 2, 0.001);
        Assert.assertEquals(agg.max(), 8, 0.001);
    }

    @Test
    public void testScore() throws Exception {
        String fileName1 = RESOURCE_PATH + "voice-t-57514.csv";
        DataList data1 = new DataList();
        data1.load(fileName1);

        String fileName2 = RESOURCE_PATH + "data-t-64946.csv";
        DataList data2 = new DataList();
        data2.load(fileName2);

        PredictScore res = new PredictScore(RESOURCE_PATH + "subs_csi_train.csv", true);
        res.update("voice", data1);
        res.update("data", data2);

        res.selectBest();
        res.printScore();

        res.predict(res.bestKof);
        res.printScore();
    }

    @Test
    public void testAddr() {
        String dateKey = String.format("%02d.%02d", 1, 2);
        String formatted = String.format("%03d", 1);
        formatted = formatted;
    }

    @Test
    public void testLte() {
        LocalDate dateNow = LocalDate.now();

        boolean res = Utils.lte(dateNow, dateNow);
        Assert.assertEquals(res, true);

        res = Utils.lte(dateNow, dateNow.minusDays(1));
        Assert.assertEquals(res, false);

        res = Utils.lte(dateNow, dateNow.plusDays(1));
        Assert.assertEquals(res, true);
    }

    @Test
    public void testGte() {
        LocalDate dateNow = LocalDate.now();

        boolean res = Utils.gte(dateNow, dateNow);
        Assert.assertEquals(res, true);

        Utils.gte(dateNow, dateNow.minusDays(1));
        Assert.assertEquals(res, true);

        res = Utils.gte(dateNow, dateNow.plusDays(1));
        Assert.assertEquals(res, false);
    }

    @Test
    public void testDate() {
        LocalDate dateNow = LocalDate.now();

        boolean res = Utils.between(dateNow, dateNow, dateNow);
        Assert.assertEquals(res, true);

        res = Utils.between(dateNow, LocalDate.MIN, LocalDate.MAX);
        Assert.assertEquals(res, true);

        res = Utils.between(dateNow, dateNow, LocalDate.MAX);
        Assert.assertEquals(res, true);

        res = Utils.between(dateNow, LocalDate.MIN, dateNow);
        Assert.assertEquals(res, true);

        res = Utils.between(dateNow, dateNow.plusDays(1), LocalDate.MAX);
        Assert.assertEquals(res, false);
    }

    @Test
    public void testDateExt() {
        DateExt dat1 = new DateExt("21.08.02");
        Assert.assertEquals(dat1.getDateKey(), "2002-08-21");

        DateExt dat2 = new DateExt("27.08");
        Assert.assertEquals(dat2.getDateKey(), "2002-08-27");

        long days = dat1.rangeDays(dat2);
        Assert.assertEquals(days, 7);

        DateExt dat3 = new DateExt("24.04 18:00:00");
        Assert.assertEquals(dat3.getDateKey(), "2002-04-24");
        dat3 = null;
    }
}
