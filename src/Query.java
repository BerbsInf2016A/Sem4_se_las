import java.util.*;
import java.util.stream.Collectors;

public class Query implements  IQuery {
    @Override
    public void executeSQL01() {

    }

    @Override
    public void executeSQL02() {

    }

    @Override
    public void executeSQL03() {

    }

    @Override
    public void executeSQL04() {

    }

    @Override
    public void executeSQL05() {

    }

    public List<Integer> executeSQL06(List<Record> records) {
        List<String> sources = Arrays.asList("a", "d");
        List<String> destinations = Arrays.asList("f", "e");
        Comparator<Record> descendingWeightComparator = (Record record1 , Record record2) -> (int)(record2.getWeight() - record1.getWeight());
        Comparator<Record> ascendingDestinationComparator = (Record record1 ,Record record2) -> (int)(record1.getDestination().compareTo(record2.getDestination()));

        List<Record> data = records.stream()
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
                .collect(Collectors.toList());
        List<Integer> result = data.stream().map(record -> record.getId()).collect(Collectors.toList());
        return result;
    }

    @Override
    public void executeSQL07() {

    }

    @Override
    public void executeSQL08() {

    }

    @Override
    public void executeSQL09() {

    }

    @Override
    public void executeSQL10() {

    }

    @Override
    public void executeSQL11() {

    }

    @Override
    public void executeSQL12() {

    }
}
