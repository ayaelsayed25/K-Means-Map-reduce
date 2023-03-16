package org.example;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements Writable {
    private int dimension;

    public int getDimension() {
        return dimension;
    }

    public float[] getFeatures() {
        return features;
    }

    private float[] features;

    public Point() {}
    public Point(float[] features)
    {
        this.features = features;
        this.dimension = features.length;
    }
    public Point(String[] features)
    {
        set(features);
    }

    public void set(String[] features)
    {
        this.dimension = features.length;
        this.features = new float[this.dimension];
        for(int i = 0; i < features.length; i++){
            this.features[i] = Float.valueOf(features[i]);
        }
    }

    //calculates Euclidean Distance
    public float distance(Point point)
    {
        float sum = 0;
        for(int i = 0; i < dimension; i++)
            sum += Math.pow(this.features[i] - point.features[i], 2);
        return (float) Math.sqrt(sum);
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(dimension);
        for(int i = 0; i < features.length; i++) {
            dataOutput.writeFloat(this.features[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.dimension = dataInput.readInt();
        this.features = new float[this.dimension];
        for(int i = 0; i < features.length; i++) {
            this.features[i] = dataInput.readFloat();
        }
    }

    @Override
    public String toString()
    {
        StringBuilder point = new StringBuilder();
        for (int i = 0; i < this.dimension; i++) {
            point.append(Float.toString(this.features[i]));
            if(i != this.dimension - 1) {
                point.append(",");
            }
        }
        return point.toString();
    }
}
