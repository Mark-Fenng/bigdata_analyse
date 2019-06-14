import java.util.*;

public abstract class Vertex<VertexValue, EdgeValue, MessageValue> {
    final private long ID;
    private VertexValue vertexValue;
    private List<Edge> outGoingEdges;
    private boolean active;

    Vertex(long vertexID) {
        this.ID = vertexID;
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

    /**
     * @return the active
     */
    public boolean isActive() {
        return active;
    }

    public void VoteToHalt() {
        this.active = false;
    }
}