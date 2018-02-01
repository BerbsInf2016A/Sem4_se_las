import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Application {

    public List<Record> loadRecords() {
            List<Record> data = new ArrayList<>();
            String fileName =  Configuration.instance.recordsFileName;

            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] strings = line.split(";");
                    data.add(new Record(Integer.parseInt(strings[0]), strings[1],  strings[2], strings[3], Integer.parseInt(strings[4]), Integer.parseInt(strings[5]),  strings[6], strings[7]));

                }
            } catch (IOException ioe) {
                System.out.println(ioe.getMessage());
            }
            return data;
    }

    /*
        --- query 01 (count)
        2 SELECT COUNT(*) FROM data
        3 1000000
         */
    @Test
    public void executeSQL01() {

        List<Record> records = loadRecords();
        long count = records.stream().count();

        Assert.assertEquals("Count should be equal",1000000,  count);
    }

    /**
     * --- query 02 (count, where)
     6 SELECT COUNT(*) FROM data WHERE source = 'a' AND destination = 'g'
     7 AND type = 'n' AND weight >= 20 AND sorter <= 5
     8 3123
     */
    // count, where
    public void executeSQL02() {
        List<Record> records = loadRecords();
        Long count = records.stream().filter(record -> record.getSource() == "a" && record.getDestination() == "g").count();
    }

    /*
    --- query 03 (count, where, in)
    11 SELECT COUNT(*) FROM data WHERE source IN ('a','c') AND destination = 'g'
    12 AND type = 'e' AND customs = 'y'
    13 3136
     */
    public void executeSQL03() {
    }

    // count, where, not in
    public void executeSQL04() {
    }

    // id, where, in, order by desc limit
    public void executeSQL05() {
    }

    // id, where, in, order by desc, order by asc
    public void executeSQL06() {
    }

    // count, group by
    public void executeSQL07() {
    }

    // count, where, group by
    public void executeSQL08() {
    }

    // count, where, in, group by
    public void executeSQL09() {
    }

    // count, where, not in, group by
    public void executeSQL10() {
    }

    // sum, where, not in, in, group by
    public void executeSQL11() {
    }

    // avg, where, in, in, group by
    public void executeSQL12() {
    }

    public void execute() {
        loadRecords();
        executeSQL01();
        executeSQL02();
        executeSQL03();
        executeSQL04();
        executeSQL05();
        executeSQL06();
        executeSQL07();
        executeSQL08();
        executeSQL09();
        executeSQL10();
        executeSQL11();
        executeSQL12();
    }

    public static void main(String... args) {

    }
}