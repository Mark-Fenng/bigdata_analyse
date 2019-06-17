import java.util.*;

public class Worker<MessageValue> implements Runnable {
    private final int WorkerID;
    // store all vertices object
    // the key represent the ID of vertex and the value represent the vertex
    private Map<Long, Vertex> vertices = new HashMap<>();
    // store queues of each vertex, which has all messages waiting to be sent to
    // other vertex
    // key represent the ID of vertex and value represent
    private Map<Long, Queue<MessageValue>> waitingMessages = new HashMap<>();
    // the time each super step cost
    private long time = 0;
    // number of messages sent to other worker during each super step
    private long sendMessagesNum = 0;
    // number of messages received during each super step
    private long receiveMessagesNum = 0;

    Worker(int workerID) {
        this.WorkerID = workerID;
    }

    /**
     * @return the workerID
     */
    public int getWorkerID() {
        return WorkerID;
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

    /**
     * @return the times
     */
    public long getTime() {
        return time;
    }

    /**
     * @return the sendMessagesNum
     */
    public long getSendMessagesNum() {
        return sendMessagesNum;
    }

    /**
     * @return the receiveMessagesNum
     */
    public long getReceiveMessagesNum() {
        return receiveMessagesNum;
    }

    /**
     * send message to Master node to inform it that this worker has finished
     * computing
     */
    synchronized public void sendMessageToMaster() {
        Master.receiveFromWorker(this.WorkerID);
    }

    /**
     * receive message from Master node and start one new super step
     */
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

    public void vertexReceiveMessage() {
        this.receiveMessagesNum += 1;
    }

    synchronized public long sendMessage() {
        long sum = 0;
        for (Map.Entry<Long, Queue<MessageValue>> entry : this.waitingMessages.entrySet()) {
            int workerID = Master.allocateVertex(entry.getKey());
            Worker<MessageValue> destWorker = Master.getWorker(workerID);
            Combiner combiner = Master.getCombiner();
            if (combiner != null) {
                if (entry.getValue().size() != 0) {
                    destWorker.getVertex(entry.getKey())
                            .receiveMessage(combiner.Combine((Queue<Object>) entry.getValue()));
                    sum += 1;
                }
            } else {
                sum += entry.getValue().size();
                Queue<MessageValue> value = entry.getValue();
                while (!value.isEmpty()) {
                    destWorker.getVertex(entry.getKey()).receiveMessage(value.remove());
                }
            }
        }
        return sum;
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
        this.sendMessagesNum = 0;
        this.receiveMessagesNum = 0;
        this.time = 0;
        long startTime = System.currentTimeMillis();
        report();
        for (Map.Entry<Long, Vertex> entry : vertices.entrySet()) {
            if (entry.getValue().isActive()) {
                entry.getValue().runCompute();
            }
        }
        this.sendMessagesNum = sendMessage();
        sendMessageToMaster();
        this.time = System.currentTimeMillis() - startTime;
    }
}