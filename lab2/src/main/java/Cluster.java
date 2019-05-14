import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public class Cluster {
    private static final int MaxIteration = 100, DATA_CENTER = 9;
    private static final double Deviation = 0.01;
    private static int count = 0;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("first").master("local[*]")
                .config("spark.some.config.option", "some-value").getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");
        String filePath = "./USCensus1990.data.txt";
        JavaRDD<String> fileRDD = sc.textFile(filePath);
        JavaRDD<Record> parseData = fileRDD.map(S -> new Record(S));
        parseData = KMeans(parseData);
        parseData.map(r -> r.getType()).repartition(1).saveAsTextFile("./result");
        spark.stop();
    }

    static JavaRDD<Record> KMeans(JavaRDD<Record> parseData) {

        int DEGREE = parseData.first().getData().size(); // data degree num'
        // store center point
        List<List<Double>> Centers = parseData.take(DATA_CENTER).stream().map(R -> R.getDoubleData())
                .collect(Collectors.toList());
        List<List<Double>> preCenters;

        // start iteration
        for (int i = 0; i < MaxIteration; i++) {
            preCenters = cloneList(Centers);
            // update the current type for each record
            parseData = parseData.map(new Function<Record, Record>() {

                private static final long serialVersionUID = 1L;

                public Record call(Record key) throws Exception {
                    double min = 99999999;
                    int min_index = -1;
                    for (int j = 0; j < DATA_CENTER; j++) {
                        double sum = 0;
                        sum = calculateDistance1(key.getData(), Centers.get(j));
                        if (sum < min) {
                            min = sum;
                            min_index = j;
                        }
                    }
                    key.setType(min_index);
                    return key;
                }
            });

            // update center positions of each cluster
            for (int j = 0; j < DATA_CENTER; j++) {
                final int tempInt = j;
                count = 0;
                List<Double> temp = parseData.filter(r -> r.getType().equals(new Integer(tempInt)))
                        .map(r -> r.getDoubleData()).reduce(new Function2<List<Double>, List<Double>, List<Double>>() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public List<Double> call(List<Double> v1, List<Double> v2) throws Exception {
                                for (int k = 0; k < DEGREE; k++) {
                                    v2.set(k, v1.get(k) + v2.get(k));
                                }
                                count++;
                                return v2;
                            }
                        });
                for (int k = 0; k < DEGREE; k++) {
                    Centers.get(j).set(k, temp.get(k) / count);
                }
            }

            System.out.println("iteration: " + i);
            double temp = calculateDistance2(Centers, preCenters);
            System.out.println("V :" + temp);
            if (temp < Deviation) {
                break;
            }
        }
        return parseData;
    }

    static List<List<Double>> cloneList(List<List<Double>> inputList) {
        List<List<Double>> result = new ArrayList<>();
        for (int i = 0; i < inputList.size(); i++) {
            List<Double> tempList = new ArrayList<>();
            tempList.addAll(inputList.get(i));
            result.add(tempList);
        }
        return result;
    }

    static double calculateDistance1(List<Integer> l1, List<Double> l2) {
        double sum = 0;
        for (int i = 0; i < l1.size(); i++) {
            sum += Math.pow((double) l1.get(i) - (double) l2.get(i), 2);
        }
        return sum;
    }

    static double calculateDistance2(List<List<Double>> l1, List<List<Double>> l2) {
        double sum = 0;
        for (int i = 0; i < l1.size(); i++) {
            for (int j = 0; j < l1.get(i).size(); j++) {
                sum += Math.pow((double) l1.get(i).get(j) - (double) l2.get(i).get(j), 2);
            }
        }
        return sum;
    }
}