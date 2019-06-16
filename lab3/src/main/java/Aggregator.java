import java.util.LinkedList;
import java.util.Queue;

public abstract class Aggregator<ReportValue, ReturnValue> {
    private Queue<ReportValue> reports = new LinkedList<>();
    private ReturnValue result = null;

    /**
     * @return the result
     */
    public ReturnValue getResult() {
        return result;
    }

    void runReport(Vertex vertex) {
        ReportValue reportValue = report(vertex);
        if (reportValue != null) {
            synchronized (this.reports) {
                this.reports.offer(reportValue);
            }
        }
    }

    void runAggregator() {
        if (reports.size() != 0) {
            result = Aggregator(this.reports);
        }
    }

    public abstract ReportValue report(Vertex vertex);

    public abstract ReturnValue Aggregator(Queue<ReportValue> reports);
}