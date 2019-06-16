import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Queue;

public class SSSP extends Vertex<Integer, Integer, Integer> {

    SSSP(long vertexID) {
        super(vertexID);
    }

    @Override
    public void Compute(Queue<Integer> messages) {
        int minDistance = isSource() ? 0 : Integer.MAX_VALUE;
        if (Master.SuperStep() == 0) {
            setVertexValue(minDistance);
            for (Edge<Integer> edge : getOutGoingEdges()) {
                sendMessage(edge.getTargetVertexID(),
                        minDistance == Integer.MAX_VALUE ? Integer.MAX_VALUE : minDistance + edge.getEdgeValue());
            }
        }
        while (!messages.isEmpty()) {
            minDistance = Math.min(minDistance, messages.remove());
        }
        if (minDistance < getVertexValue()) {
            setVertexValue(minDistance);
            for (Edge<Integer> edge : getOutGoingEdges()) {
                sendMessage(edge.getTargetVertexID(), minDistance + edge.getEdgeValue());
            }
        }
        VoteToHalt();
    }

    public static void main(String[] args) throws Exception {
        SSSPCombiner combiner = new SSSPCombiner();
        Master.setCombiner(combiner);
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
            if (!Master.vertexExist(sourceVertexID)) {
                SSSP sourceVertex = new SSSP(sourceVertexID);
                Master.addVertex(sourceVertex);
            }
            if (!Master.vertexExist(targetVertexID)) {
                SSSP targetVertex = new SSSP(targetVertexID);
                Master.addVertex(targetVertex);
            }
            Edge<Integer> edge = new Edge(1, targetVertexID);
            edge.setEdgeValue(1);
            Master.addEdge(sourceVertexID, edge);
        }
        // System.out.println("The number of Vertices: " + Master.NumVertices());
        // System.out.println("The number of edges: " + Master.NumEdges());
        System.out.println("Start running graph algorithm...");
        Master.startNewSuperStep();
        // System.out.println("Finished graph algorithm!!!");
        bufferedReader.close();
    }

}