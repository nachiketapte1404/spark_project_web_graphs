package com.nachiket;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.TriangleCount;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag$;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Graph_Metrics_App {
    public static void main(String[] args) throws IOException{
        // Initialize SparkConf
        SparkConf conf = new SparkConf().setAppName("Graph Metrics");
        // Initialize JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        String[] paths = {"hdfs://nachiket:50000/datasets/web-Google.txt", "hdfs://nachiket:50000/datasets/web-NotreDame.txt"};
        int ind = 1;
        String outputFilePath = "/home/hadoop/Documents/spark_project_1/src/main/metrics_results.txt";
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));
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
            Graph<Object, Integer> triangleGraph = TriangleCount.run(graph,
                    ClassTag$.MODULE$.apply(Integer.class),
                    ClassTag$.MODULE$.apply(Integer.class));

            // nodesCount
            long nodesCount = graph.vertices().count();

            // Edges count
            long edgesCount = graph.edges().count();

            // Nodes in largest connected component (WCC)
            long nodesInLargestWCC = nodesCount;
            double fractionNodesInLargestWCC = (double) nodesInLargestWCC / nodesCount;

            // Edges in largest connected component (WCC)
            long edgesInLargestWCC = edgesCount;
            double fractionEdgesInLargestWCC = (double) edgesInLargestWCC / edgesCount;

            // Number of triangles
            long numTriangles = triangleGraph.triplets().count();
            // Fraction of closed triangles
            double fractionClosedTriangles = numTriangles / (double) nodesCount;

            // Print results
            writer.write("\nStats for graph " + ind++ + ": ");
            writer.write("\nNodes: " + nodesCount);
            writer.write("\nEdges: " + edgesCount);
            writer.write(
                    "\nNodes in largest WCC: " + nodesInLargestWCC + " (" + fractionNodesInLargestWCC + ")");
            writer.write(
                    "\nEdges in largest WCC: " + edgesInLargestWCC + " (" + fractionEdgesInLargestWCC + ")");
            writer.write("\nNumber of triangles: " + numTriangles);
            writer.write("\nFraction of closed triangles: " + fractionClosedTriangles);

            writer.write("\n\n\n");
        }
        // Stop SparkContext
        writer.close();
        sc.stop();
        sc.close();
    }
}