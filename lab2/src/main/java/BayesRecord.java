import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class BayesRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    private List<Double> data = new ArrayList<>();
    private int type = -1;

    BayesRecord(String rawRecord) {
        List<String> strList = Arrays.asList(rawRecord.split(","));
        this.data = strList.subList(1, strList.size()).stream().map(Double::parseDouble).collect(Collectors.toList());
        this.type = (int) Double.parseDouble(strList.get(0));
    }

    /**
     * @return the type
     */
    public int getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(int type) {
        this.type = type;
    }

    /**
     * @return the data
     */
    public List<Double> getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(List<Double> data) {
        this.data = data;
    }

}