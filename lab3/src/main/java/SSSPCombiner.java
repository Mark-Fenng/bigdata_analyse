import java.util.Queue;

public class SSSPCombiner extends Combiner<Object> {

    @Override
    public Object Combine(Queue<Object> messages) {
        int minDistance = Integer.MAX_VALUE;
        while (!messages.isEmpty())
            minDistance = Math.min(minDistance, (int) messages.remove());
        return minDistance;
    }

}