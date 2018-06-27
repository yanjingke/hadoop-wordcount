package com.itscat.mr.wcdemo;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.FileOutputStream;
import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(WordCountCombiner.class);//配置combiner

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //如果不设置InputFormat，他默认TextInputformat
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        CombineTextInputFormat.setMinInputSplitSize(job,2097152);

        //指定job输入原始文件所在的目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //指定job输出原始文件所在的目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
        //com.itscat.mr.wcdemo.WordCountDriver.java
    }
}
