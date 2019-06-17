import java.util.Queue;

public class NumVertices extends Aggregator<Integer, Integer> {

    @Override
    public Integer report(Vertex vertex) {
        return 1;
    }

    @Override
    public Integer Aggregator(Queue<Integer> reports) {
        int sum = 0;
        while (!reports.isEmpty()) {
            sum += reports.remove();
        }
        return sum;
    }
}