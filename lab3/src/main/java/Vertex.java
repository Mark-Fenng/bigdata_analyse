import java.util.*;

public abstract class Vertex<VertexValue, EdgeValue, MessageValue> {
    final private long ID;
    private Worker worker;
    private VertexValue vertexValue;
    private List<Edge<EdgeValue>> outGoingEdges = new ArrayList<>();
    private Queue<MessageValue> messageQueue1 = new LinkedList<>(), messageQueue2 = new LinkedList<>();
    private boolean active = true;

    Vertex(long vertexID) {
        this.ID = vertexID;
    }

    /**
     * @return the worker
     */
    public Worker getWorker() {
        return worker;
    }

    /**
     * @param worker the worker to set
     */
    public void setWorker(Worker worker) {
        this.worker = worker;
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
        if (Master.SuperStep() % 2 != 0) {
            synchronized (messageQueue2) {
                this.messageQueue2.offer(message);
            }
        } else {
            synchronized (messageQueue1) {
                this.messageQueue1.offer(message);
            }
        }
    }

    public void sendMessage(long destID, MessageValue message) {
        worker.addQueue(destID, message);
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
        if (Master.SuperStep() % 2 != 0) {
            return active || this.messageQueue1.size() != 0;
        } else {
            return active || this.messageQueue2.size() != 0;
        }
    }

    public void VoteToHalt() {
        this.active = false;
    }

    public void sendMessageToAllNeighbors(MessageValue message) {
        for (Edge<EdgeValue> edge : outGoingEdges) {
            sendMessage(edge.getTargetVertexID(), message);
        }
    }

    public boolean isSource() {
        return getID() == 0;
    }

    public void runCompute() {
        if (Master.SuperStep() % 2 != 0) {
            Compute(this.messageQueue1);
        } else {
            Compute(this.messageQueue2);
        }
    }

    public abstract void Compute(Queue<MessageValue> messages);
}