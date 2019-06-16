import java.util.*;

public class Worker<MessageValue> implements Runnable {
    private final int WorkerID;
    private Map<Long, Vertex> vertices = new HashMap<>();
    private Map<Long, Queue<MessageValue>> waitingMessages = new HashMap<>();

    Worker(int workerID) {
        this.WorkerID = workerID;
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

    /**
     * @return the vertices
     */
    public Map<Long, Vertex> getVertices() {
        return vertices;
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

    synchronized public void sendMessageToMaster() {
        Master.receiveFromWorker(this.WorkerID);
    }

    public void receiveFromMaster() {
        if (vertices.values().stream().filter(s -> s.isActive()).count() > 0) {
            Thread worker = new Thread(this);
            worker.start();
        } else {
            Master.workerEnd();
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

    synchronized public void sendMessage() {
        for (Map.Entry<Long, Queue<MessageValue>> entry : this.waitingMessages.entrySet()) {
            int workerID = Master.allocateVertex(entry.getKey());
            Worker<MessageValue> destWorker = Master.getWorker(workerID);
            Combiner combiner = Master.getCombiner();
            if (combiner != null) {
                if (entry.getValue().size() != 0) {
                    destWorker.getVertex(entry.getKey())
                            .receiveMessage(combiner.Combine((Queue<Object>) entry.getValue()));
                }
            } else {
                Queue<MessageValue> value = entry.getValue();
                while (!value.isEmpty()) {
                    destWorker.getVertex(entry.getKey()).receiveMessage(value.remove());
                }
            }
        }
    }

    synchronized public void report() {
        for (Aggregator<Object, Object> aggregator : Master.getAggregators().values())
            for (Vertex vertex : vertices.values()) {
                if (aggregator.getResult() == null)
                    aggregator.runReport(vertex);
            }
    }

    @Override
    public void run() {
        report();
        for (Map.Entry<Long, Vertex> entry : vertices.entrySet()) {
            if (entry.getValue().isActive()) {
                entry.getValue().runCompute();
            }
        }
        sendMessage();
        sendMessageToMaster();
    }
}