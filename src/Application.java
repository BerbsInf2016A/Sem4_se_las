import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Application {


    private static List<Record> cachedRecords;

    private List<Record> copyRecordData() {
        List<Record> newList = new ArrayList<>();
        for(Record p : this.cachedRecords) {
            newList.add(p.clone());
        }
        return newList;
    }

    public List<Record> loadRecords() {

        if (this.cachedRecords != null) return this.copyRecordData();
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
        if (this.cachedRecords == null) this.cachedRecords = data;
        return data;
    }

    /*
        --- query 01 (count)
        2 SELECT COUNT(*) FROM data
        3 1000000
         */
    @Test
    public void executeSQL01() {
        long count = Query.instance.executeSQL01(this.loadRecords());

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
        long count = Query.instance.executeSQL02(this.loadRecords());
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
        long count = Query.instance.executeSQL03(this.loadRecords());

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

        long count = Query.instance.executeSQL04(this.loadRecords());

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
        List<Integer> idsOrderByWeightDescendingLimit3 = Query.instance.executeSQL05(this.loadRecords());

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
        List<Integer> result = Query.instance.executeSQL06(this.loadRecords());

        List<Integer> expectedResult = Arrays.asList(158036, 188829, 196332, 289290, 937204, 491565, 500654, 108316, 282370, 422002, 540879
                ,563094, 625456, 685382, 252566, 495325);

        Assert.assertEquals("Results should be the same", expectedResult, result);
    }

    /**
     * --- query 07 (count, group by)
     50 SELECT customs,COUNT(*) FROM data GROUP BY customs
     51 n 899935
     52 y 100065
     */
    // count, group by
    @Test
    public void executeSQL07() {
        Map<String, Long> result = Query.instance.executeSQL07(this.loadRecords());

        Map<String, Long> expectedResult = new HashMap<String, Long>()
        {{
            put("n", 899935L);
            put("y", 100065L);
        }};


        Assert.assertEquals("Results should be the same", expectedResult, result);
    }

    /**
     * --- query 08 (count, where, group by)
     55 SELECT sorter,COUNT(*) FROM data WHERE customs = 'y' AND extendedSecurityCheck = 'y'
     56 GROUP BY sorter
     57 1 257
     58 11 252
     59 6 255
     60 5 263
     61 12 231
     62 9 255
     63 2 270
     64 4 254
     65 7 248
     66 8 257
     67 3 230
     68 10 244
     */
    // count, where, group by
    @Test
    public void executeSQL08() {
        Map<Integer, Long> result = Query.instance.executeSQL08(this.loadRecords());

        Map<Integer, Long> expectedResult = new HashMap<Integer, Long>()
        {{
            put(1, 257L);
            put(11, 252L);
            put(6, 255L);
            put(5, 263L);
            put(12, 231L);
            put(9, 255L);
            put(2, 270L);
            put(4, 254L);
            put(7, 248L);
            put(8, 257L);
            put(3, 230L);
            put(10, 244L);
        }};

        Assert.assertEquals("Results should be the same", expectedResult, result);
    }

    @Test
    public void executeSQL09() {
        Map<String, Long> result = Query.instance.executeSQL09(this.loadRecords());

        Map<String, Long> expected = new HashMap<>();
        expected.put("f", 1513L);
        expected.put("h", 1511L);
        expected.put("g", 1498L);

        Assert.assertEquals("Should be equal", expected, result);
    }

    /*
    82 --- query 10 (count, where, not in, group by)
    83 SELECT extendedSecurityCheck,COUNT(*) FROM data WHERE source = 'a'
    84 AND destination = 'f' AND type NOT IN ('b','e') AND customs = 'n'
    85 AND sorter = 8 GROUP BY extendedSecurityCheck
    86 n 2239
    87 y 64
     */
    @Test
    public void executeSQL10() {

        Map<String, Long> result = Query.instance.executeSQL10(this.loadRecords());
        Map<String, Long> expected = new HashMap<>();
        expected.put("n", 2239L);
        expected.put("y", 64L);

        Assert.assertEquals("Should be equal", expected, result);
    }

    /**
     --- query 11 (sum, where, not in, in, group by)
     90 SELECT sorter,SUM(weight) FROM data WHERE source NOT IN ('a','c')
     91 AND destination = 'h' AND sorter IN (1,3,5,6) AND customs = 'y'
     92 AND extendedSecurityCheck = 'n' GROUP BY sorter
     93 1 18755
     94 3 18922
     95 6 18739
     96 5 19273
     */
    // sum, where, not in, in, group by
    @Test
    public void executeSQL11() {
        Map<Integer, Integer> result = Query.instance.executeSQL11(this.loadRecords());

        Map<Integer, Integer> expectedResult = new HashMap<Integer, Integer>()
        {{
            put(1, 18755);
            put(3, 18922);
            put(6, 18739);
            put(5, 19273);
        }};

        Assert.assertEquals("Results should be the same", expectedResult, result);
    }

    /*
   --- query 12 (avg, where, in, in, group by)
   99 SELECT destination,AVG(weight) FROM data WHERE source IN ('a','b')
   100 AND destination IN ('f','h') AND extendedSecurityCheck = 'n'
   101 GROUP BY destination
   102 h 18
   103 f 19
    */
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