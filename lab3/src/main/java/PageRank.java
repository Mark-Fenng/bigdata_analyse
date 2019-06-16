import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Queue;

public class PageRank extends Vertex<Double, Double, Double> {

    PageRank(long vertexID) {
        super(vertexID);
    }

    @Override
    public void Compute(Queue<Double> messages) {
        if (Master.SuperStep() >= 1) {
            double sum = 0;
            while (!messages.isEmpty()) {
                sum += messages.remove();
            }
            Integer result = (Integer) Master.getAggregator("NumVertices").getResult();
            super.setVertexValue(0.15 / result + 0.85 * sum);
        }
        if (Master.SuperStep() < 30) {
            final long n = getOutGoingEdges().size();
            sendMessageToAllNeighbors(super.getVertexValue() / n);
        } else {
            VoteToHalt();
        }
    }

    public static void main(String[] args) throws Exception {
        Master.createWorker(3);
        Aggregator<Integer, Integer> aggregator = new NumVertices();
        Master.addAggregator("NumVertices", aggregator);

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
        // System.out.println("The number of Vertices: " + Master.NumVertices());
        // System.out.println("The number of edges: " + Master.NumEdges());
        System.out.println("Start running graph algorithm...");
        Master.startNewSuperStep();
        bufferedReader.close();
    }

}