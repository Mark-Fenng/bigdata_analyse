import java.util.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Classification {
    static int DEGREE = 0;

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder().appName("first").master("local[*]")
                .config("spark.some.config.option", "some-value").getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");
        String filePath = "./SUSY.csv";
        // JavaRDD<String> fileInput = sc.textFile(filePath);
        Dataset<Row> inputData = spark.read().format("com.databricks.spark.csv").option("header", false)
                .option("delimiter", ",").option("inferSchema", true).load(filePath);
        // inputData.printSchema();
        DEGREE = inputData.columns().length - 1;
        inputData.createTempView("records");

        Dataset<Row> trainData = inputData.sample(false, 0.0001, 1000);
        // Dataset<Row> testData = rawData.sample(false, 0.2, 1000);
        prioriProbability(spark, trainData);
        spark.stop();
    }

    static Map<Integer, Double> prioriProbability(SparkSession spark, Dataset<Row> trainData) {
        long totalNum = trainData.count();
        String sql = "select _c0,";
        // for (int i = 1; i <= DEGREE; i++) {
        // sql += "mean(_c" + i + "),";
        // sql += "stddev(_c" + i + "),";
        // }
        sql += "count(_c1)/" + totalNum + " as p from records group by _c0";
        Dataset<Row> prioriProbability = spark.sql(sql).persist();
        prioriProbability.show();
        int typeNum = (int) prioriProbability.count();
        JavaRDD<List<Double>> midResult = prioriProbability.toJavaRDD().map(new Function<Row, List<Double>>() {
            public List<Double> call(Row key) throws Exception {
                List<Double> result = new ArrayList<>();
                result.add(key.getDouble(0));
                result.add(key.getDouble(1));
                return result;
            }
        });
        midResult.take(typeNum);
        System.out.println(midResult.toString());
        // prioriProbability.unpersist();
        return null;
    }

    // static
}