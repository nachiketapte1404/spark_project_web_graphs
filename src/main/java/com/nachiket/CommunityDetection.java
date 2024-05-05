package com.nachiket;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.graphx.*;
import scala.Tuple2;
import scala.reflect.ClassTag$;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.storage.StorageLevel;
import java.io.IOException;

public class CommunityDetection 
{

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Community Detection");
        // Initialize JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        int ind = 1;
        String outputFilePath = "/home/hadoop/Documents/spark_project_1/src/main/community_detection_results3.txt";
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));
        
            // Load data from HDFS into Spark RDD
            JavaRDD<String> linesRDD = sc.textFile("hdfs://nachiket:50000/datasets/web-NotreDame.txt");

            // Parse data and construct edges RDD
            JavaPairRDD<Long, Long> adjacencyListRDD = linesRDD.mapToPair(line -> {
                String[] parts = line.split("\\s+");
                return new Tuple2<>(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
            });

            // Convert adjacency list to edges RDD
            JavaRDD<Edge<Object>> edgesRDD = adjacencyListRDD.map(edge -> new Edge<>(edge._1(), edge._2(), 1));

            // Create directed graph
            Graph<Object, Object> graph = Graph.fromEdges(edgesRDD.rdd(), 0, StorageLevel.MEMORY_AND_DISK(),
                    StorageLevel.MEMORY_AND_DISK(), ClassTag$.MODULE$.apply(Object.class),
                    ClassTag$.MODULE$.apply(Object.class));
            System.out.println("Passed stage 1");
            Graph<Object, Object> communities = ConnectedComponents.run(graph, ClassTag$.MODULE$.apply(Object.class),
                    ClassTag$.MODULE$.apply(Object.class));
            System.out.println("Passed stage 2");
            // Get the vertices with their corresponding community ID
            JavaRDD<Tuple2<Object, Object>> verticesWithCommunity = communities.vertices().toJavaRDD();
            System.out.println("Passed stage 3");
            JavaPairRDD<Object, Integer> communityCounts = verticesWithCommunity
                    .mapToPair(tuple -> new Tuple2<>(tuple._2(), 1))
                    .reduceByKey((count1, count2) -> count1 + count2);
            System.out.println("Passed stage 4");

            // Collect and print the number of communities and the number of nodes in each
            // community
            List<Tuple2<Object, Integer>> communityCountsList = communityCounts.collect();
            System.out.println("Passed stage 5");
            writer.write("\nFor graph " + ind++ +" : ");
            writer.write("\nNumber of communities: " + communityCountsList.size());
            for (Tuple2<Object, Integer> tuple : communityCountsList) {
                writer.write("\nCommunity ID: " + tuple._1() + " has " + tuple._2() + " nodes");
            }
        writer.close();
        sc.stop();
        sc.close();
    }
}