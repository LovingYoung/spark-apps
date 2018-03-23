package src.main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];

        SparkConf conf = new SparkConf().setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> words = input.flatMap((r) -> Arrays.asList(r.split(" ")).iterator());

        JavaPairRDD<String, Integer> counts = words
                .mapToPair((s) -> new Tuple2<>(s.toLowerCase(), 1))
                .reduceByKey((r, s) -> r + s);
        counts.saveAsTextFile(outputFile);
    }
}
