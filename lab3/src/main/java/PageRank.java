import java.util.Queue;

public class PageRank extends Vertex<Double, Double, Double> {

    PageRank(long vertexID) {
        super(vertexID);
    }

    @Override
    public void Compute(Queue<Double> messages) {
        if (super.SuperStep() >= 1) {
            double sum = 0;
            while (!messages.isEmpty()) {
                sum += messages.poll();
            }
            super.setVertexValue(0.15 / Master.NumVertices() + 0.85 * sum);
        }
        if (super.SuperStep() < 30) {
            final long n = getOutGoingEdges().size();
            sendMessageToAllNeighbors(super.getVertexValue() / n);
        } else {
            VoteToHalt();
        }
    }

}