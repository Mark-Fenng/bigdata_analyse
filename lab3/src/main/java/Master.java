import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Master {
    private static List<Worker> Workers = new ArrayList<>();
    private static List<Boolean> WorkingFlags = new ArrayList<>();
    private static long superStep = 0;

    private static int calculatingNodes = 0;

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

    public static long SuperStep() {
        return superStep;
    }

    public static void main(String[] args) throws Exception {
        Master.createWorker(3);

        // Directed graph (each unordered pair of nodes is saved once): web-Google.txt
        // Web graph from the Google programming contest, 2002
        // Nodes: 875713 Edges: 5105039
        // FromNodeId ToNodeId
        String FilePath = "./web-Google.txt";

        BufferedReader bufferedReader = new BufferedReader(new FileReader(FilePath));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] edgeRecord = line.split("\\s+");
            long sourceVertexID = Long.parseLong(edgeRecord[0]);
            long targetVertexID = Long.parseLong(edgeRecord[1]);
            if (!vertexExist(sourceVertexID)) {
                PageRank sourceVertex = new PageRank(sourceVertexID);
                sourceVertex.setVertexValue((double) 0);
                addVertex(sourceVertex);
            }
            if (!vertexExist(targetVertexID)) {
                PageRank targetVertex = new PageRank(targetVertexID);
                targetVertex.setVertexValue((double) 0);
                addVertex(targetVertex);
            }
            Edge<Double> edge = new Edge(0, targetVertexID);
            addEdge(sourceVertexID, edge);
        }
        System.out.println("The number of Vertices: " + NumVertices());
        System.out.println("The number of edges: " + NumEdges());
        System.out.println("Start running graph algorithm...");
        startNewSuperStep();
        System.out.println("Finished graph algorithm!!!");
        bufferedReader.close();
    }
}