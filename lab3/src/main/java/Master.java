import java.util.*;

public class Master {
    private List<Worker> Workers = new ArrayList<>();

    Master(int WorkersNumber) {
        createWorker(WorkersNumber);
    }

    public long allocateVertex(long vertexID) {
        int WorkersNum = Workers.size();
        return vertexID % WorkersNum;
    }

    private void createWorker(int N) {
        for (int i = 0; i < N; i++) {
            Workers.add(new Worker());
        }
    }

    public Worker getWorker(int workerID) {
        return Workers.get(workerID);
    }

    public static void main(String[] args) {
    }
}