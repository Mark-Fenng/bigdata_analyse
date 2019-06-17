import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Master {
    // store all workers
    private static List<Worker> Workers = new ArrayList<>();
    // store flags corresponding to Workers, which indicate if each Worker has
    // stopped running
    private static List<Boolean> WorkingFlags = new ArrayList<>();
    // record super step number
    private static long superStep = 0;
    // store the number of workers which have been inactive stopped running
    private static int endedWorkers = 0;
    // store the number of workers which have stopped running
    private static int calculatingNodes = 0;
    // store one Combiner object overridden by users
    private static Combiner<Object> Combiner = null;
    // store all aggregators overridden by users
    // String means its unique name Aggregator represent its object
    private static Map<String, Aggregator<Object, Object>> aggregators = new HashMap();

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
            for (Worker worker : Workers) {
                System.out.println("Worker " + worker.getWorkerID() + " using time: " + worker.getTime());
            }
            for (Worker worker : Workers) {
                System.out.println(
                        "Worker " + worker.getWorkerID() + " sent messages number: " + worker.getSendMessagesNum());
            }
            for (Worker worker : Workers) {
                System.out.println("Worker " + worker.getWorkerID() + " received messages number: "
                        + worker.getReceiveMessagesNum());
            }
            System.out.println();
            startNewSuperStep();
        }
    }

    public static boolean checkSuperStepOver() {
        return calculatingNodes == Workers.size();
    }

    public static void runAggregators() {
        for (Aggregator aggregator : aggregators.values()) {
            if (aggregator.getResult() == null)
                aggregator.runAggregator();
        }
    }

    public static void startNewSuperStep() {
        runAggregators();
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

    public static void printStatisticsMessage() {
        System.out.println("Statistics message: ");
        for (Worker worker : Workers) {
            System.out.println("Worker ID: " + worker.getWorkerID());
            System.out.println("Number of vertices: " + worker.getVerticesNum());
            System.out.println("Number of edges: " + worker.getEdgesNum());
        }
    }

    /**
     * get current super step num
     * 
     * @return long the super step num
     */
    public static long SuperStep() {
        return superStep;
    }

    public static void addAggregator(String name, Aggregator aggregator) {
        aggregators.put(name, aggregator);
    }

    /**
     * @return the aggregators
     */
    public static Map<String, Aggregator<Object, Object>> getAggregators() {
        return aggregators;
    }

    /**
     * @return the aggregators
     */
    public static Aggregator<Object, Object> getAggregator(String name) {
        return aggregators.get(name);
    }

    public static void save() throws IOException {
        String filePath = "./nodes/worker";
        BufferedWriter bufferedWriter;
        for (Worker worker : Workers) {
            bufferedWriter = new BufferedWriter(new FileWriter(filePath + worker.getWorkerID()));
            for (Object item : worker.getVertices().values()) {
                Vertex vertex = (Vertex) item;
                for (Object edge : vertex.getOutGoingEdges()) {
                    bufferedWriter.write(String.format("%d\t%d\n", vertex.getID(), ((Edge) edge).getTargetVertexID()));
                }
            }
            bufferedWriter.close();
        }
    }

    public static void load() throws IOException {
        String FilePath = "./nodes/worker";
        BufferedReader bufferedReader = null;
        for (Worker worker : Workers) {
            bufferedReader = new BufferedReader(new FileReader(FilePath + worker.getWorkerID()));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] edgeRecord = line.split("\\s+");
                long sourceVertexID = Long.parseLong(edgeRecord[0]);
                long targetVertexID = Long.parseLong(edgeRecord[1]);
                if (!Master.vertexExist(sourceVertexID)) {
                    PageRank sourceVertex = new PageRank(sourceVertexID);
                    sourceVertex.setVertexValue((double) 0);
                    Master.addVertex(sourceVertex);
                }
                if (!Master.vertexExist(targetVertexID)) {
                    PageRank targetVertex = new PageRank(targetVertexID);
                    targetVertex.setVertexValue((double) 0);
                    Master.addVertex(targetVertex);
                }
                Edge<Double> edge = new Edge(0, targetVertexID);
                Master.addEdge(sourceVertexID, edge);
            }
            bufferedReader.close();
        }
    }
}