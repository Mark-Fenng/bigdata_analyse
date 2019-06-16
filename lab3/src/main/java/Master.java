import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Master {
    private static List<Worker> Workers = new ArrayList<>();
    private static List<Boolean> WorkingFlags = new ArrayList<>();
    private static long superStep = 0;
    private static int endedWorkers = 0;
    private static Combiner<Object> Combiner = null;

    private static int calculatingNodes = 0;

    /**
     * @return the combiner
     */
    public static Combiner<Object> getCombiner() {
        return Combiner;
    }

    /**
     * @param combiner the combiner to set
     */
    public static void setCombiner(Combiner<Object> combiner) {
        Combiner = combiner;
    }

    public static boolean vertexExist(long vertexID) {
        int workerID = allocateVertex(vertexID);
        return getWorker(workerID).vertexExist(vertexID);
    }

    public static void addVertex(Vertex vertex) {
        int workerID = allocateVertex(vertex.getID());
        getWorker(workerID).addVertex(vertex);
    }

    public static void addEdge(long vertexID, Edge edge) {
        int workerID = allocateVertex(vertexID);
        getWorker(workerID).addEdge(vertexID, edge);
    }

    public static int allocateVertex(long vertexID) {
        int WorkersNum = Workers.size();
        return (int) (vertexID % WorkersNum);
    }

    public static void createWorker(int N) {
        for (int i = 0; i < N; i++) {
            Workers.add(new Worker(i));
            WorkingFlags.add(false);
        }
    }

    synchronized public static void workerEnd() {
        endedWorkers += 1;
        if (endedWorkers == Workers.size()) {
            endSuperStep();
        }
    }

    public static Worker getWorker(int workerID) {
        return Workers.get(workerID);
    }

    synchronized public static void receiveFromWorker(int workerID) {
        if (WorkingFlags.get(workerID)) {
            WorkingFlags.set(workerID, false);
            calculatingNodes += 1;
        }
        if (checkSuperStepOver()) {
            superStep += 1;
            System.out.println("Super step: " + superStep);
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

    public static long NumEdges() {
        long edgesNum = 0;
        for (int i = 0; i < Workers.size(); i++) {
            edgesNum += Workers.get(i).getEdgesNum();
        }
        return edgesNum;
    }

    public static void endSuperStep() {
        try {
            System.out.println("Finished graph algorithm!!!");
            printVertices("./result.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void printVertices(String FilePath) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(FilePath));
        String format = "vertexID= %d\tvalue= %s";
        for (Worker worker : Workers) {
            for (Object item : worker.getVertices().values()) {
                Vertex vertex = (Vertex) item;
                bufferedWriter.write(format.format(format, vertex.getID(), vertex.getVertexValue()));
                bufferedWriter.newLine();
            }
        }
        bufferedWriter.close();
    }

    public static long SuperStep() {
        return superStep;
    }
}