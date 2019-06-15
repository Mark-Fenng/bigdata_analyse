import java.util.*;

public class Worker {
    private final int WorkerID;
    private Map<Long, Vertex> vertices = new HashMap<>();
    private Map<Long, Thread> threads = new HashMap<>();
    private int workingVertex = 0;

    Worker(int workerID) {
        this.WorkerID = workerID;
    }

    void addVertex(Vertex newVertex) {
        this.vertices.put(newVertex.getID(), newVertex);
        this.threads.put(newVertex.getID(), new Thread(newVertex));
    }

    public long getVerticesNum() {
        return this.vertices.size();
    }

    public Vertex getVertex(long vertexID) {
        return this.vertices.get(vertexID);
    }

    public void sendMessageToMaster() {
        Master.receiveFromWorker(this.WorkerID);
    }

    public void addWorkingThread() {
        this.workingVertex += 1;
    }

    public void reduceWorkingThread() {
        this.workingVertex -= 1;
        if (workingVertex == 0) {
            sendMessageToMaster();
        }
    }

    public void receiveFromMaster() {
        for (Map.Entry<Long, Vertex> entry : vertices.entrySet()) {
            if (entry.getValue().isActive()) {
                threads.get(entry.getKey()).start();
                addWorkingThread();
            }
        }
    }

}