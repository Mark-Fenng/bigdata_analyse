import java.util.*;

public class Worker implements Runnable {
    private final int WorkerID;
    private Map<Long, Vertex> vertices = new HashMap<>();

    Worker(int workerID) {
        this.WorkerID = workerID;
    }

    boolean vertexExist(long vertexID) {
        return this.vertices.containsKey(vertexID);
    }

    void addVertex(Vertex newVertex) {
        if (!this.vertices.containsKey(newVertex.getID())) {
            newVertex.setWorkerID(this.WorkerID);
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
        Thread worker = new Thread(this);
        worker.start();
    }

    @Override
    public void run() {
        for (Map.Entry<Long, Vertex> entry : vertices.entrySet()) {
            if (entry.getValue().isActive()) {
                entry.getValue().runCompute();
            }
        }
        sendMessageToMaster();
    }
}