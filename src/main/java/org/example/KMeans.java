package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;

public class KMeans {

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    //get input and output files from  args
    String inputFile = args[0];
    String outFile = args[1] + "/temp";
    //configure the values needed
    Configuration config = new Configuration();
    config.addResource(new Path("configuration.xml"));
    int numOfRecords = config.getInt("dataset.records", 150);
    int k = config.getInt("k", 3);
    float threshold = config.getFloat("threshold", 0.05f);
    int maxNumberOfIterations = config.getInt("max.iteration", 30);

    Point[] oldCentroids;
    Point[] newCentroids;
    //initialize k centroids
    newCentroids = initializeCentroids(config, inputFile, k, numOfRecords);
    boolean stopIteration = false;
    boolean check;
    int iteration = 0;
    boolean succeded = true;
    while(!stopIteration)
    {
      //configure the job
      Job job = Job.getInstance(config, "iteration_" + iteration);
      job.setJarByClass(KMeans.class);
      job.setMapperClass(KMeansMapper.class);
      job.setReducerClass(KMeansReducer.class);
      job.setNumReduceTasks(k); //one task each centroid
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Point.class);
      FileInputFormat.addInputPath(job, new Path(inputFile));
      FileOutputFormat.setOutputPath(job, new Path(outFile));
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      //wait for job to complete
      succeded = job.waitForCompletion(true);
      //If the job fails the application will be closed.
      if(!succeded) {
        System.err.println("Iteration " + iteration + "failed.");
        System.exit(1);
      }
      //copy the centroids from last job to oldCentroids
      oldCentroids = newCentroids;
      //get the new centroids calculated by the mapreduce
      //from the output file
      newCentroids = readNewCentroids(config, k, outFile);
      //check if distance between old centroids and new centroids
      //is less than or equal to the threshold
      check = checkThreshold(oldCentroids, newCentroids, threshold, k);
      stopIteration = check || iteration >= maxNumberOfIterations - 1;
      if(stopIteration)
        WriteFinalOutput(config, newCentroids, args[1], k);
      else{
        //set the new centroids to the config file to be used in the nex jobs
        //by the mapper and reducer
        for(int j = 0; j < k; j++)
        {
          config.unset("centroid." + j);
          config.set("centroid." + j, newCentroids[j].toString());
        }
      }
      iteration++;
    }

  }

  private static void WriteFinalOutput(Configuration config, Point[] newCentroids, String outFile, int k) throws IOException {
    FileSystem hdfs = FileSystem.get(config);
    FSDataOutputStream dos = hdfs.create(new Path(outFile + "/centroids.txt"), true);
    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(dos));

    for(int i = 0; i < k; i++) {
      bufferedWriter.write(newCentroids[i].toString());
      bufferedWriter.newLine();
    }
    bufferedWriter.close();
    hdfs.close();
  }

  private static boolean checkThreshold(Point[] oldCentroids, Point[] newCentroids, double threshold, int k) {
    for(int i = 0; i < k; i++)
    {
      float dist = oldCentroids[i].distance(newCentroids[i]);
      if(dist <= threshold)
        return true;
    }
    return false;
  }

  private static Point[] readNewCentroids(Configuration config, int k, String outFile) throws IOException {
    Point[] centroids = new Point[k];
    FileSystem hdfs = FileSystem.get(config);
    //get all k output files from reducer
    FileStatus[] status = hdfs.listStatus(new Path(outFile));
    for(int i = 0; i < k; i++)
    {
      if(!status[i].getPath().toString().endsWith("_SUCCESS")) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
        String line = bufferedReader.readLine();
        String[] keyValue = line.split("\t");
        int centroidId = Integer.parseInt(keyValue[0]);
        String[] values = keyValue[1].split(",");
        centroids[centroidId] = new Point(values);
        bufferedReader.close();
      }
    }
    hdfs.delete(new Path(outFile), true);
    return centroids;
  }

  private static Point[] initializeCentroids(Configuration config, String inputFile, int k, int numOfRecords) throws IOException {
    Point[] centroids = new Point[k];
    LinkedList<Integer> randomIndices = new LinkedList<>();
    Random rand = new Random();
    //create random k indices
    for(int i = 0; i < k; i++)
      randomIndices.add(rand.nextInt(numOfRecords));
    //sort the indices
    Collections.sort(randomIndices);
    //read the k random centroids from input file
    FileSystem hdfs = FileSystem.get(config);
    FSDataInputStream inputStream = hdfs.open(new Path(inputFile));
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
    int recordIndex = 0;
    int index = 0;
    while(index < randomIndices.size())
    {
      String point = bufferedReader.readLine();
      if(recordIndex == randomIndices.get(index))
      {
        String[] line = point.split(",");
        centroids[index] = new Point(Arrays.copyOfRange(line, 0, line.length - 1));
        index++;
      }
      recordIndex++;
    }
    bufferedReader.close();
    return centroids;
  }
}