import java.util.*;
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

    @Override
    public void executeSQL07(List<Record> records) {
        
    }

    @Override
    public void executeSQL08(List<Record> records) {

    }

    @Override
    public void executeSQL09(List<Record> records) {

    }

    @Override
    public void executeSQL10(List<Record> records) {

    }

    @Override
    public void executeSQL11(List<Record> records) {

    }

    @Override
    public void executeSQL12(List<Record> records) {

    }


}
