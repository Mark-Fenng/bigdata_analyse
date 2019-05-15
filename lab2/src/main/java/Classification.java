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

        Dataset<Row> trainData = inputData.sample(false, 0.8, 1000);
        System.out.println("Start training");
        List<List<Double>> BayesModel = prioriProbability(spark, trainData);
        System.out.println("Start testing");
        Dataset<Row> testData = inputData.sample(false, 0.2, 2000);
        long testTotal = testData.count();
        int result = testData.toJavaRDD().map(new Function<Row, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Row key) throws Exception {
                double max = -10000;
                int max_index = -1;
                for (int i = 0; i < BayesModel.size(); i++) {
                    double r = 1;
                    for (int j = 0; j < DEGREE; j++) {
                        r = r * (1 / (Math.sqrt(2 * Math.PI) * BayesModel.get(i).get(1 + 2 * j + 1))
                                * Math.exp(-(Math.pow(key.getDouble(1 + j) - BayesModel.get(i).get(1 + 2 * j), 2) / 2
                                        / Math.pow(BayesModel.get(i).get(1 + 2 * j + 1), 2))));
                    }
                    r *= BayesModel.get(i).get(2 * (DEGREE + 1) - 1);
                    if (r > max) {
                        max_index = i;
                    }
                }
                return (int) key.getDouble(0) == max_index ? 1 : 0;
            }
        }).reduce((a, b) -> a + b);
        System.out.println(result / (double) testTotal);
        spark.stop();
    }

    static List<List<Double>> prioriProbability(SparkSession spark, Dataset<Row> trainData) throws AnalysisException {
        trainData.createTempView("records");
        long totalNum = trainData.count();
        String sql = "select _c0,";
        for (int i = 1; i <= DEGREE; i++) {
            sql += "mean(_c" + i + "),";
            sql += "stddev(_c" + i + "),";
        }
        sql += "count(_c1)/" + totalNum + " as p from records group by _c0";
        Dataset<Row> prioriProbability = spark.sql(sql).persist();
        // prioriProbability.show();
        int typeNum = (int) prioriProbability.count();
        JavaRDD<List<Double>> midResult = prioriProbability.toJavaRDD().map(new Function<Row, List<Double>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public List<Double> call(Row key) throws Exception {
                List<Double> result = new ArrayList<>();
                for (int i = 0; i < 2 * (DEGREE + 1); i++) {
                    result.add(key.getDouble(i));
                }
                return result;
            }
        });
        List<List<Double>> result = midResult.take(typeNum);
        prioriProbability.unpersist();
        return result;
    }
}