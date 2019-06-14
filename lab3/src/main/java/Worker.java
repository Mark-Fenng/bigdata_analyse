import java.util.*;

public class Worker {
    private Map<Long, Vertex> vertices = new HashMap<>();

    void addVertex(Vertex newVertex) {
        this.vertices.put(newVertex.getID(), newVertex);
    }

}