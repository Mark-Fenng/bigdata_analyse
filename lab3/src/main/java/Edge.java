class Edge<EdgeValue> {
    private EdgeValue edgeValue;
    private long targetVertexID;

    Edge(EdgeValue value, long targetVertexID) {
        this.edgeValue = value;
        this.targetVertexID = targetVertexID;
    }

    /**
     * @return the edgeValue
     */
    public EdgeValue getEdgeValue() {
        return edgeValue;
    }

    /**
     * @param edgeValue the edgeValue to set
     */
    public void setEdgeValue(EdgeValue edgeValue) {
        this.edgeValue = edgeValue;
    }

    /**
     * @return the targetVertex
     */
    public long getTargetVertexID() {
        return this.targetVertexID;
    }
}