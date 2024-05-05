package com.nachiket;

import java.util.List;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.graphx.*;
import scala.Tuple2;
import scala.reflect.ClassTag;
import java.util.Comparator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.reflect.ClassTag$;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

class PageRankComparator implements Comparator<Tuple2<Object, Object>>, Serializable {
    @Override
    public int compare(Tuple2<Object, Object> tuple1, Tuple2<Object, Object> tuple2) {
        Double rank1 = (Double) tuple1._2();
        Double rank2 = (Double) tuple2._2();
        return -rank1.compareTo(rank2); // Sorting in descending order
    }
}

public class PageRank1 {
    public static void main(String[] args) throws IOException{
        // Initialize SparkConf
        SparkConf conf = new SparkConf().setAppName("Graph Metrics");
        // Initialize JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        String[] paths = { "hdfs://nachiket:50000/datasets/web-BerkStan.txt",
                "hdfs://nachiket:50000/datasets/web-Google.txt", "hdfs://nachiket:50000/datasets/web-NotreDame.txt",
                "hdfs://nachiket:50000/datasets/web-Stanford.txt" };
                String outputFilePath = "/home/hadoop/Documents/spark_project_1/src/main/pagerank_results.txt";
                BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));
            int ind = 1;
        for (String path : paths) {

            // Load data from HDFS into Spark RDD
            JavaRDD<String> linesRDD = sc.textFile(path);

            // Parse data and construct edges RDD
            JavaPairRDD<Long, Long> adjacencyListRDD = linesRDD.mapToPair(line -> {
                String[] parts = line.split("\\s+");
                return new Tuple2<>(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
            });

            // Convert adjacency list to edges RDD
            JavaRDD<Edge<Integer>> edgesRDD = adjacencyListRDD.map(edge -> new Edge<>(edge._1(), edge._2(), 1));

            // Create directed graph
            Graph<Integer, Integer> graph = Graph.fromEdges(edgesRDD.rdd(), 0, StorageLevel.MEMORY_AND_DISK(),
                    StorageLevel.MEMORY_AND_DISK(), ClassTag$.MODULE$.apply(Integer.class),
                    ClassTag$.MODULE$.apply(Integer.class));
            // Convert integer vertex and edge types to object vertex and edge types
            // Convert integer vertex and edge types to object vertex and edge types
            // Run PageRank algorithm
            ClassTag<Integer> objectClassTag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);
            Graph<Object, Object> ranks = PageRank.run(graph, 10, 0.0001, objectClassTag, objectClassTag);

            // Get PageRank scores
            JavaRDD<Tuple2<Object, Object>> pageRanks = ranks.vertices().toJavaRDD();
            List<Tuple2<Object, Object>> pageRankList = pageRanks.takeOrdered(10, new PageRankComparator());
            writer.write("For graph " + ind++ + ": ");
            writer.write("\nTop 10 PageRank scores: ");
            for (Tuple2<Object, Object> tuple : pageRankList) {
                writer.write("\nVertex: " + tuple._1() + " has PageRank score: " + tuple._2());
            }
        }
        writer.close();
        sc.stop();
        sc.close();
    }
}