package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
    private Point[] centroids;
    private final Point point = new Point();
    private final IntWritable centroid = new IntWritable();

    protected void setup(Context context)
    {
        Configuration conf = context.getConfiguration();
        int k = Integer.parseInt(conf.get("k"));
        this.centroids = new Point[k];
        for(int i = 0; i < k; i++) {
            String[] centroid = context.getConfiguration().getStrings("centroid." + i);
            this.centroids[i] = new Point(centroid);
        }

    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        String[] features = Arrays.copyOfRange(line, 0, line.length - 1);
        point.set(features);
        float distance;
        float minDistance = Float.POSITIVE_INFINITY;
        int nearestCentroidIndex = 0;
        //find the nearest centroid
        for(int i = 0; i < centroids.length; i++)
        {
            distance = point.distance(centroids[i]);
            if(distance < minDistance)
            {
                minDistance = distance;
                nearestCentroidIndex = i;
            }
        }
        centroid.set(nearestCentroidIndex);
        context.write(centroid, point);
    }
}
