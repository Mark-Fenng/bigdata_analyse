import java.util.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Classification {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("first").master("local[*]")
                .config("spark.some.config.option", "some-value").getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");
        String filePath = "./SUSY.csv";
        JavaRDD<String> fileInput = sc.textFile(filePath);
        JavaRDD<BayesRecord> dataset = fileInput.map(r -> new BayesRecord(r));
        Dataset<Row> rawData = spark.createDataFrame(dataset, BayesRecord.class);
        Dataset<Row> trainData = rawData.sample(false, 0.8, 1000);
        // Dataset<Row> testData = rawData.sample(false, 0.2, 1000);
        System.out.println(prioriProbability(trainData).toString());
        // test.takeAsList(10).forEach(System.out::println);
        // String filePath = "./USCensus1990.data.txt";
        // JavaRDD<String> fileRDD = sc.textFile(filePath);
        // JavaRDD<Record> parseData = fileRDD.map(S -> new Record(S));
        // parseData.map(r -> r.getType()).repartition(1).saveAsTextFile("./result");
        spark.stop();
    }

    static Map<Integer, Double> prioriProbability(Dataset<Row> trainData) {
        long totalNum = trainData.count();
        Dataset<Row> prioriProbability = trainData.groupBy("type").count().selectExpr("type", "count/" + totalNum)
                .persist();
        int typeNum = (int) prioriProbability.count();
        List<Tuple2<Integer, Double>> midResult = prioriProbability.toJavaRDD()
                .map(r -> new Tuple2<Integer, Double>((int) r.get(0), (double) r.get(1))).take(typeNum);
        prioriProbability.unpersist();
        HashMap<Integer, Double> Result = new HashMap<>();
        for (Tuple2<Integer, Double> t : midResult) {
            Result.put((int) t._1(), (double) t._2());
        }
        return Result;
    }
}