import java.util.*;

public class Master {
    private static List<Worker> Workers = new ArrayList<>();
    private static List<Boolean> WorkingFlags = new ArrayList<>();

    private static int calculatingNodes = 0;

    public static int allocateVertex(long vertexID) {
        int WorkersNum = Workers.size();
        return (int) (vertexID % WorkersNum);
    }

    private static void createWorker(int N) {
        for (int i = 0; i < N; i++) {
            Workers.add(new Worker(i));
            WorkingFlags.add(false);
        }
    }

    public static Worker getWorker(int workerID) {
        return Workers.get(workerID);
    }

    public static void receiveFromWorker(int workerID) {
        if (WorkingFlags.get(workerID)) {
            WorkingFlags.set(workerID, false);
            calculatingNodes += 1;
        }
        if (checkSuperStepOver()) {
            startNewSuperStep();
        }
    }

    public static boolean checkSuperStepOver() {
        return calculatingNodes == Workers.size();
    }

    public static void startNewSuperStep() {
        calculatingNodes = 0;
        for (int i = 0; i < WorkingFlags.size(); i++) {
            WorkingFlags.set(i, true);
            Workers.get(i).receiveFromMaster();
        }
    }

    public static long NumVertices() {
        long verticesNum = 0;
        for (int i = 0; i < Workers.size(); i++) {
            verticesNum += Workers.get(i).getVerticesNum();
        }
        return verticesNum;
    }

    public static void main(String[] args) {
        Master.createWorker(3);
        startNewSuperStep();
    }
}