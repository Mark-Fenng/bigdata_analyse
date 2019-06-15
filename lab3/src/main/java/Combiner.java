import java.util.*;

public abstract class Combiner<MessageValue> {
    public abstract MessageValue Combine(Queue<MessageValue> messages);
}