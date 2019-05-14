import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Record implements Serializable {
    private static final long serialVersionUID = 1L;
    private int ID; // case id
    private List<Integer> data = new ArrayList<>(); // data list
    private Integer type = 0; // cluster type
    private String format = "ID: %d %s type: %d";

    Record(String rawRecord) {
        List<String> strList = Arrays.asList(rawRecord.split(","));
        this.data = strList.subList(1, strList.size()).stream().map(Integer::parseInt).collect(Collectors.toList());
        this.ID = Integer.parseInt(strList.get(0));
    }

    /**
     * @return the iD
     */
    public int getID() {
        return ID;
    }

    /**
     * @param iD the iD to set
     */
    public void setID(int iD) {
        ID = iD;
    }

    /**
     * @return the data
     */
    public List<Integer> getData() {
        return data;
    }

    public List<Double> getDoubleData() {
        List<Double> result = new ArrayList<>();
        for (Integer var : this.data) {
            result.add((double) var);
        }
        return result;
    }

    /**
     * @param data the data to set
     */
    public void setData(List<Integer> data) {
        this.data = data;
    }

    /**
     * @return the type
     */
    public Integer getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(Integer type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format(this.format, this.ID, this.data.toString(), type);
    }
}