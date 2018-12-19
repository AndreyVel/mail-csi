package mail.csi;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * Created by demo on 5/11/2018.
 */
public class UserSci {
    private static final Logger log = LoggerFactory.getLogger(UserSci.class);
    private List<RowSci> list = new ArrayList();

    public List<RowSci> rows() {
        return list;
    }

    public HashMap<Integer, RowSci> map = new HashMap<>();

    public void loadData(String fileName) throws Exception {
        log.warn("Loading file: {}...", fileName);

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line = br.readLine();

            while ((line = br.readLine()) != null) {
                RowSci row = new RowSci(line);
                map.put(row.SK_ID, row);
                list.add(row);
            }
        }

        log.warn("Loading file: {}, {} rows...", fileName, list.size());
    }
}
