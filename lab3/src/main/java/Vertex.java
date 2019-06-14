public abstract class Vertex<VertexValue, EdgeValue, MessageValue> {
    final private long ID;
    private VertexValue vertexValue;
    private List<Edge> outGoingEdges;
    private boolean active;

    /**
     * @return the iD
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

    public VoteToHalt(){
        this.active = false;
    }
}