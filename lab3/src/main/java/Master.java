public class Master {
    private List<Worker> Workers = new ArrayList<>();

    Master(int WorkersNumber) {
        createWorker(WorkerNumber);
    }

    public int allocateVertex(long vertexID) {
        int WorkersNum = Workers.size();
    }

    private void createWorker(int N) {
        for (int i = 0; i < N; i++) {
            Workers.add(new Worker());
        }
    }

    public Worker getWorker(int workerID) {
        return Workers.get(workerID);
    }
}