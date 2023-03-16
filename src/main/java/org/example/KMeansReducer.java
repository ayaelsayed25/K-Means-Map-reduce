package org.example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer <IntWritable, Point, Text, Text>{
    private final Text centroidId = new Text();
    private final Text centroidValue = new Text();

    public void reduce(IntWritable centroid, Iterable<Point> points, Context context) throws IOException, InterruptedException {
        Point point = points.iterator().next();
        float[] features = new float[point.getDimension()];
        int pointNum = 0;
        do {
            pointNum++;
            float[] pointFeatures = point.getFeatures();
            for(int i = 0; i < point.getDimension(); i++)
            {
                features[i] += pointFeatures[i];
            }
            if(points.iterator().hasNext())
                point = points.iterator().next();
            else break;
        }while(true);
        for(int i = 0; i < features.length; i++)
        {
            features[i] = features[i] / pointNum;
        }
        centroidId.set(centroid.toString());
        centroidValue.set(new Point(features).toString());
        context.write(centroidId, centroidValue);
    }
}
