import java.util.*;

public abstract class Vertex<VertexValue, EdgeValue, MessageValue> {
    private int workerID;
    final private long ID;
    private VertexValue vertexValue;
    private List<Edge<EdgeValue>> outGoingEdges = new ArrayList<>();
    private long superStep = 0;
    private Queue<MessageValue> messageQueue1 = new LinkedList<>(), messageQueue2 = new LinkedList<>();
    private boolean odd = false;
    private boolean active = true;

    Vertex(long vertexID) {
        this.ID = vertexID;
    }

    /**
     * @param workerID the workerID to set
     */
    public void setWorkerID(int workerID) {
        this.workerID = workerID;
    }

    /**
     * @return the ID
     */
    public long getID() {
        return ID;
    }

    /**
     * @return the vertexValue
     */
    public VertexValue getVertexValue() {
        return vertexValue;
    }

    /**
     * @param vertexValue the vertexValue to set
     */
    public void setVertexValue(VertexValue vertexValue) {
        this.vertexValue = vertexValue;
    }

    public void addOutGoingEdge(Edge<EdgeValue> edge) {
        this.outGoingEdges.add(edge);
    }

    public void receiveMessage(MessageValue message) {
        if (this.odd) {
            this.messageQueue2.offer(message);
        } else {
            this.messageQueue1.offer(message);
        }
    }

    public void sendMessage(long destID, MessageValue message) {
        int workerID = Master.allocateVertex(destID);
        Worker destWorker = Master.getWorker(workerID);
        destWorker.getVertex(destID).receiveMessage(message);
    }

    /**
     * @return the outGoingEdges
     */
    public List<Edge<EdgeValue>> getOutGoingEdges() {
        return outGoingEdges;
    }

    /**
     * @return the active
     */
    public boolean isActive() {
        if (this.odd) {
            return active || this.messageQueue1.size() != 0;
        } else {
            return active || this.messageQueue2.size() != 0;
        }

    }

    public void VoteToHalt() {
        this.active = false;
    }

    public long SuperStep() {
        return this.superStep;
    }

    public void sendMessageToAllNeighbors(MessageValue message) {
        for (Edge<EdgeValue> edge : outGoingEdges) {
            sendMessage(edge.getTargetVertexID(), message);
        }
    }

    public void runCompute() {
        if (odd) {
            Compute(this.messageQueue1);
            this.messageQueue1.clear();
        } else {
            Compute(this.messageQueue2);
            this.messageQueue2.clear();
        }
        this.superStep += 1;
        this.odd = !this.odd;
    }

    public abstract void Compute(Queue<MessageValue> messages);
}