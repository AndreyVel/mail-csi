package mail.csi;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.List;

public class RowSci {
    public int SK_ID; // Уникальный идентификатор абонента
    public int CSI = 0; // Оценка воспринимаемого качества сервисов - целевой признак
    public DateExt CONTACT_DATE; // Дата опроса CSI абонента

    // SK_ID;CSI;CONTACT_DATE
    // 1973;0;13.05
    // SK_ID;CONTACT_DATE

    public RowSci(String line) {
        List<String> data = Lists.newArrayList(Splitter.on(";").split(line));

        String monDay = null;
        SK_ID = Integer.parseInt(data.get(0));

        if (data.size() == 3) {
            CSI = Integer.parseInt(data.get(1));
            monDay = data.get(2);
        } else if (data.size() == 2) {
            SK_ID = Integer.parseInt(data.get(0));
            monDay = data.get(1);
        } else {
            throw new RuntimeException("Bad value: " + line);
        }

        CONTACT_DATE = new DateExt(monDay);
    }
}
