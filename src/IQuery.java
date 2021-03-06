import java.util.List;
import java.util.Map;

public interface IQuery {
    long executeSQL01(List<Record> records);
    long executeSQL02(List<Record> records);
    long executeSQL03(List<Record> records);
    long executeSQL04(List<Record> records);
    List<Integer> executeSQL05(List<Record> records);
    List<Integer> executeSQL06(List<Record> records);
    Map<String, Long> executeSQL07(List<Record> records);
    Map<Integer, Long> executeSQL08(List<Record> records);
    Map<String, Long> executeSQL09(List<Record> records);
    Map<String, Long> executeSQL10(List<Record> records);
    Map<Integer, Integer> executeSQL11(List<Record> records);
    Map<String, Integer> executeSQL12(List<Record> records);
}