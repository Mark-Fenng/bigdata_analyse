import java.util.*;

public class Worker<MessageValue> implements Runnable {
    private final int WorkerID;
    private Map<Long, Vertex> vertices = new HashMap<>();
    private Map<Long, Queue<MessageValue>> waitingMessages = new HashMap<>();
    private Combiner<MessageValue> Combiner = null;

    Worker(int workerID) {
        this.WorkerID = workerID;
    }

    /**
     * @param combiner the combiner to set
     */
    public void setCombiner(Combiner<MessageValue> combiner) {
        Combiner = combiner;
    }

    boolean vertexExist(long vertexID) {
        return this.vertices.containsKey(vertexID);
    }

    void addVertex(Vertex newVertex) {
        if (!this.vertices.containsKey(newVertex.getID())) {
            newVertex.setWorker(this);
            this.vertices.put(newVertex.getID(), newVertex);
        }
    }

    void addEdge(long vertexID, Edge edge) {
        vertices.get(vertexID).addOutGoingEdge(edge);
    }

    public long getVerticesNum() {
        return this.vertices.size();
    }

    public long getEdgesNum() {
        long edgesNum = 0;
        for (Map.Entry<Long, Vertex> entry : vertices.entrySet()) {
            edgesNum += entry.getValue().getOutGoingEdges().size();
        }
        return edgesNum;
    }

    public Vertex getVertex(long vertexID) {
        return this.vertices.get(vertexID);
    }

    public void sendMessageToMaster() {
        Master.receiveFromWorker(this.WorkerID);
    }

    public void receiveFromMaster() {
        if (vertices.values().stream().filter(s -> s.isActive()).count() > 0) {
            Thread worker = new Thread(this);
            worker.start();
        }

    }

    public void addQueue(long destVertexID, MessageValue message) {
        if (!this.waitingMessages.containsKey(destVertexID)) {
            Queue<MessageValue> tempQueue = new LinkedList<>();
            tempQueue.offer(message);
            this.waitingMessages.put(destVertexID, tempQueue);
        } else {
            this.waitingMessages.get(destVertexID).offer(message);
        }
    }

    public void sendMessage() {
        for (Map.Entry<Long, Queue<MessageValue>> entry : this.waitingMessages.entrySet()) {
            int workerID = Master.allocateVertex(entry.getKey());
            Worker<MessageValue> destWorker = Master.getWorker(workerID);
            if (Combiner != null) {
                destWorker.getVertex(entry.getKey()).receiveMessage(Combiner.Combine(entry.getValue()));
            } else {
                Queue<MessageValue> value = entry.getValue();
                while (!value.isEmpty()) {
                    destWorker.getVertex(entry.getKey()).receiveMessage(value.remove());
                }
            }
        }
    }

    @Override
    public void run() {
        for (Map.Entry<Long, Vertex> entry : vertices.entrySet()) {
            if (entry.getValue().isActive()) {
                entry.getValue().runCompute();
            }
        }
        sendMessage();
        sendMessageToMaster();
    }
}