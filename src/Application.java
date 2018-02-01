import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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
    @Test
    public void executeSQL02() {
        List<Record> records = loadRecords();
        long count = records.stream()
                .filter(record -> record.getSource().toLowerCase().equals("a"))
                .filter(record -> record.getDestination().toLowerCase().equals("g"))
                .filter(record -> record.getType().toLowerCase().equals("n"))
                .filter(record -> record.getWeight() >= 20)
                .filter(record -> record.getSorter() <= 5)
                .count();

        Assert.assertEquals("Count should be equal", 3123, count);
    }

    /*
    --- query 03 (count, where, in)
    11 SELECT COUNT(*) FROM data WHERE source IN ('a','c') AND destination = 'g'
    12 AND type = 'e' AND customs = 'y'
    13 3136
     */
    @Test
    public void executeSQL03() {
        List<Record> records = loadRecords();
        long count = records.stream()
                .filter(record -> record.getSource().toLowerCase().equals("a") || record.getSource().toLowerCase().equals("c"))
                .filter(record -> record.getDestination().toLowerCase().equals("g"))
                .filter(record -> record.getType().toLowerCase().equals("e"))
                .filter(record -> record.getCustoms().toLowerCase().equals("y"))
                .count();

        Assert.assertEquals("Count should be equal",3136,  count);
    }

    /*
    --- query 04 (count, where, not in)
    16 SELECT COUNT(*) FROM data WHERE source = 'b' AND destination NOT IN ('f','h')
    17 AND type = 'n' AND customs = 'n'
    18 28246
     */
    @Test
    public void executeSQL04() {
        List<Record> records = loadRecords();
        long count = records.stream()
                .filter(record -> record.getSource().toLowerCase().equals("b"))
                .filter(record -> !( record.getDestination().toLowerCase().equals("f") || record.getDestination().toLowerCase().equals("h") ) )
                .filter(record -> record.getType().toLowerCase().equals("n"))
                .filter(record -> record.getCustoms().toLowerCase().equals("n"))
                .count();

        Assert.assertEquals("Count should be equal",28246,  count);
    }

    /*
     --- query 05 (id, where, in, order by desc limit)
     21 SELECT id FROM data WHERE source IN ('b','c') AND destination = 'g' AND type = 'n'
     22 AND sorter <= 5 AND customs = 'y' AND extendedSecurityCheck = 'y'
     23 ORDER BY weight DESC LIMIT 3
     24 357530
     25 59471
     26 136168
     */
    @Test
    // id, where, in, order by desc limit
    public void executeSQL05() {
        List<Record> records = loadRecords();
        Comparator<Record> descendingByWeight = (Record record1, Record record2) -> (record2.getWeight() - record1.getWeight());

        List<Integer> idsOrderByWeightDescendingLimit3 = records.stream()
                .filter(record -> record.getSource().toLowerCase().equals("b") || record.getSource().toLowerCase().equals("c"))
                .filter(record -> record.getDestination().toLowerCase().equals("g"))
                .filter(record -> record.getType().toLowerCase().equals("n"))
                .filter(record -> record.getSorter() <= 5)
                .filter(record -> record.getCustoms().toLowerCase().equals("y"))
                .filter(record -> record.getExtendedSecurityCheck().toLowerCase().equals("y"))
                .sorted(descendingByWeight)
                .map(record -> record.getId())
                .limit(3)
                .collect(Collectors.toList());

        Assert.assertEquals("Elements should be the same with the same order",Arrays.asList(357530,59471,136168), idsOrderByWeightDescendingLimit3);
    }

    /*
    --- query 06 (id, where, in, order by desc, order by asc)
    29 SELECT id FROM data WHERE source IN ('a','d') AND destination IN ('f','e')
    30 AND type = 'b' AND weight >= 29 AND customs = 'y' AND extendedSecurityCheck = 'y'
    31 ORDER BY weight DESC, destination
    32 158036
    33 188829
    34 196332
    35 289290
    36 937204
    37 491565
    38 500654
    39 108316
    40 282370
    41 422002
    42 540879
    43 563094
    44 625456
    45 685382
    46 252566
    47 495325
     */
    @Test
    public void executeSQL06() {
        List<Integer> result = new Query().executeSQL06(loadRecords());

        List<Integer> expectedResult = Arrays.asList(158036, 188829, 196332, 289290, 937204, 491565, 500654, 108316, 282370, 422002, 540879
                ,563094, 625456, 685382, 252566, 495325);

        Assert.assertEquals("Results should be the same", expectedResult, result);
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