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
            super.setVertexValue(0.15 / Master.NumVertices() + 0.85 * sum);
        }
        if (Master.SuperStep() < 30) {
            final long n = getOutGoingEdges().size();
            sendMessageToAllNeighbors(super.getVertexValue() / n);
        } else {
            VoteToHalt();
        }
    }

}