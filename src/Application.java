import java.util.ArrayList;
import java.util.List;

public class Application {
    private List<Record> records = new ArrayList<>();

    public List<Record> loadRecords() {
        try {
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // count
    public void executeSQL01() {
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

    // count, where, in
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