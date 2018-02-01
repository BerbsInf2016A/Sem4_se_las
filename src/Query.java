import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Query implements  IQuery {
    public static Query instance = new Query();

    public long executeSQL01(List<Record> records) {
        return records.stream().count();
    }

    public long executeSQL02(List<Record> records) {
        return records.stream()
                .filter(record -> record.getSource().toLowerCase().equals("a"))
                .filter(record -> record.getDestination().toLowerCase().equals("g"))
                .filter(record -> record.getType().toLowerCase().equals("n"))
                .filter(record -> record.getWeight() >= 20)
                .filter(record -> record.getSorter() <= 5)
                .count();

    }

    public long executeSQL03(List<Record> records) {
        return records.stream()
                .filter(record -> record.getSource().toLowerCase().equals("a") || record.getSource().toLowerCase().equals("c"))
                .filter(record -> record.getDestination().toLowerCase().equals("g"))
                .filter(record -> record.getType().toLowerCase().equals("e"))
                .filter(record -> record.getCustoms().toLowerCase().equals("y"))
                .count();
    }

    public long executeSQL04(List<Record> records) {
        return records.stream()
                .filter(record -> record.getSource().toLowerCase().equals("b"))
                .filter(record -> !( record.getDestination().toLowerCase().equals("f") || record.getDestination().toLowerCase().equals("h") ) )
                .filter(record -> record.getType().toLowerCase().equals("n"))
                .filter(record -> record.getCustoms().toLowerCase().equals("n"))
                .count();
    }

    public List<Integer> executeSQL05(List<Record> records) {
        Comparator<Record> descendingByWeight = (Record record1, Record record2) -> (record2.getWeight() - record1.getWeight());

        return records.stream()
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
    }

    public List<Integer> executeSQL06(List<Record> records) {
        List<String> sources = Arrays.asList("a", "d");
        List<String> destinations = Arrays.asList("f", "e");
        Comparator<Record> descendingWeightComparator = (Record record1 , Record record2) -> (int)(record2.getWeight() - record1.getWeight());
        Comparator<Record> ascendingDestinationComparator = (Record record1 ,Record record2) -> (int)(record1.getDestination().compareTo(record2.getDestination()));

        return records.stream()
                .filter(record -> sources.contains(record.getSource().toLowerCase()))
                .filter(record -> destinations.contains(record.getDestination().toLowerCase()))
                .filter(record -> record.getType().toLowerCase().equals("b"))
                .filter(record -> record.getWeight() >= 29)
                .filter(record -> record.getCustoms().toLowerCase().equals("y"))
                .filter(record -> record.getExtendedSecurityCheck().toLowerCase().equals("y"))
                .sorted(ascendingDestinationComparator)
                .collect(Collectors.toList())
                .stream()
                .sorted(descendingWeightComparator)
                .map(record -> record.getId()).collect(Collectors.toList());
    }

    /**
     * --- query 07 (count, group by)
     50 SELECT customs,COUNT(*) FROM data GROUP BY customs
     51 n 899935
     52 y 100065
     * @param records
     * @return
     */
    public Map<String, Long> executeSQL07(List<Record> records) {
        Map<String, Long> result = new HashMap<>();
        records.stream()
                .collect(Collectors.groupingBy(Record::getCustoms))
                .forEach((k, v) -> result.put(k, v.stream().count()));
        return result;
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
     * @param records
     */
    public Map<Integer, Long> executeSQL08(List<Record> records) {
        Map<Integer, Long> result = new HashMap<>();
        records.stream()
                .filter(record -> record.getCustoms().toLowerCase().equals("y"))
                .filter(record -> record.getExtendedSecurityCheck().toLowerCase().equals("y"))
                .collect(Collectors.groupingBy(Record::getSorter))
                .forEach((k, v) -> result.put(k, v.stream().count()));
        return result;
    }

    /*
   --- query 09 (count, where, in, group by)
   75 SELECT destination,COUNT(*) FROM data WHERE source = 'c'
   76 AND destination IN ('f','g','h') AND type = 'n' AND customs = 'y'
   77 AND extendedSecurityCheck = 'n' GROUP BY destination
   78 f 1513
   79 h 1511
   80 g 1498
    */
    public Map<String, Long> executeSQL09(List<Record> records) {
        List<String> destinations = Arrays.asList("f", "g", "h");

        Map<String, Long> result = new HashMap<>();
        records.stream()
                .filter(record -> record.getSource().toLowerCase().equals("c"))
                .filter(record -> destinations.contains(record.getDestination().toLowerCase()))
                .filter(record -> record.getType().toLowerCase().equals("n"))
                .filter(record -> record.getCustoms().toLowerCase().equals("y"))
                .filter(record -> record.getExtendedSecurityCheck().toLowerCase().equals("n"))
                .collect(Collectors.groupingBy(Record::getDestination))
                .forEach((k,v) -> result.put(k,v.stream().count()));

        return result;
    }

    /*
    82 --- query 10 (count, where, not in, group by)
    83 SELECT extendedSecurityCheck,COUNT(*) FROM data WHERE source = 'a'
    84 AND destination = 'f' AND type NOT IN ('b','e') AND customs = 'n'
    85 AND sorter = 8 GROUP BY extendedSecurityCheck
    86 n 2239
    87 y 64
     */
    public Map<String, Long> executeSQL10(List<Record> records) {
        Predicate<Record> notInBorE = record -> ! (record.getType().toLowerCase().equals("b") || record.getType().toLowerCase().equals("e"));
        Map<String, Long> result = new HashMap<>();
        records.stream()
                .filter(record -> record.getSource().toLowerCase().equals("a"))
                .filter(record -> record.getDestination().toLowerCase().equals("f"))
                .filter(notInBorE)
                .filter(record -> record.getCustoms().toLowerCase().equals("n"))
                .filter(record -> record.getSorter() == 8)
                .collect(Collectors.groupingBy(Record::getExtendedSecurityCheck))
                .forEach((k,v) -> result.put(k,v.stream().count()));
        return result;
    }


    /**
     * --- query 11 (sum, where, not in, in, group by)
     90 SELECT sorter,SUM(weight) FROM data WHERE
     source NOT IN ('a','c')
     91 AND destination = 'h'
     AND sorter IN (1,3,5,6)
     AND customs = 'y'
     92 AND extendedSecurityCheck = 'n'
     GROUP BY sorter
     93 1 18755
     94 3 18922
     95 6 18739
     96 5 19273
     * @param records
     * @return
     */
    public Map<Integer, Integer> executeSQL11(List<Record> records) {
        Map<Integer, Integer> result = new HashMap<>();
        List<Integer> sorters = Arrays.asList(1,3,5,6);
        List<String> forbiddenSources = Arrays.asList("a", "c");

        records.stream()
                .filter(record -> !forbiddenSources.contains(record.getSource().toLowerCase()))
                .filter(record -> record.getDestination().toLowerCase().equals("h"))
                .filter(record -> sorters.contains(record.getSorter()))
                .filter(record -> record.getCustoms().toLowerCase().equals("y"))
                .filter(record -> record.getExtendedSecurityCheck().toLowerCase().equals("n"))
                .collect(Collectors.groupingBy(Record::getSorter))
                .forEach((k, v) -> result.put(k, v.stream().collect(Collectors.summingInt(record -> ((Integer)record.getWeight())))));

        return result;
    }

    /*
    --- query 12 (avg, where, in, in, group by)
    99 SELECT destination,AVG(weight) FROM data WHERE source IN ('a','b')
    100 AND destination IN ('f','h') AND extendedSecurityCheck = 'n'
    101 GROUP BY destination
    102 h 18
    103 f 19
     */
    public Map<String, Integer> executeSQL12(List<Record> records) {
        List<String> sources = Arrays.asList("a", "b");
        List<String> destinations = Arrays.asList("f", "h");
        Map<String, OptionalDouble> result = new HashMap<>();


        records.stream()
                .filter(record -> sources.contains(record.getSource().toLowerCase()))
                .filter(record -> destinations.contains(record.getDestination().toLowerCase()))
                .filter(record -> record.getExtendedSecurityCheck().toLowerCase().equals("n"))
                .collect(Collectors.groupingBy(Record::getDestination))
                .forEach((k, v) -> result.put(k, (v.stream().mapToDouble(record -> (record.getWeight())).average()));

    }


}
