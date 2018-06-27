package com.itscat.mr.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class WordCountCombiner  extends Reducer<Text,IntWritable,Text,IntWritable> {
    IntWritable intWritable= new IntWritable();
    @Override

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(IntWritable v:values){
            count+=v.get();
        }
        intWritable.set( count);
        context.write(key,intWritable);
    }
}
