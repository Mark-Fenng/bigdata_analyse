class Edge<EdgeValue> {
    private EdgeValue edgeValue;
    private Vertex targetVertex;

    Edge(EdgeValue value, Vertex targetVertex) {
        this.edgeValue = value;
        this.targetVertex = targetVertex;
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
        return targetVertex.getID();
    }
}