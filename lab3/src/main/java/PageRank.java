import java.util.Queue;

public class PageRank extends Vertex<Double, Double, Double> {

    PageRank(long vertexID, int workerID) {
        super(vertexID, workerID);
    }

    @Override
    public void Compute(Queue<Double> messages) {
        if (super.SuperStep() >= 1) {
            double sum = 0;
            for (; !messages.isEmpty(); messages.poll())
                sum += messages.peek();
            super.setVertexValue(0.15 / Master.NumVertices() + 0.85 * sum);
        }
        if (super.SuperStep() < 30) {
            final long n = getOutGoingEdges().size();
            sendMessageToAllNeighbors(getVertexValue() / n);
        } else {
            VoteToHalt();
        }
    }

}