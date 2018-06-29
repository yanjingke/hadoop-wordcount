package com.itacast.mr.mapsidejoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapSideJoin {
    /*setup在map之前调用详解看map源码
    * */
    static class MapSideJoinMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        Map<String,String>pdInForMap=new HashMap<String,String>();
       Text k =new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
          //  BufferedReader br = new BufferedReader(new FileReader("pdts.txt"));
            String line;
            while (StringUtils.isNotEmpty(line=br.readLine())){
                String[] fields = line.split(",");
                pdInForMap.put(fields[0],fields[1]+"/t"+fields[2]+"/t"+fields[3]);
            }
            br.close();
        }
//在map中就可以使用join
        @Override

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String oderline=value.toString();
            String[]fileds=oderline.split(",");
            String pdNameandPriceandsum=pdInForMap.get(fileds[2]);
            k.set(oderline+"/t"+pdNameandPriceandsum);
            context.write(k,NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job=Job.getInstance(configuration);
        job.setJarByClass( MapSideJoin .class);
        job.setMapperClass(MapSideJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //指定需要缓存一个文件到所有maptask运行节点到工作目录
        //job.addArchiveToClassPath("路径");缓存jar包到task运行classpath中
        //job.addCacheArchive(“lujing”);缓存压缩包到运行节点的工作目录
        //job.addCacheFile(“路径”);缓存普通文件到运行节点的工作目录
        //job.addArchiveToClassPath("路径");缓存普通文件到运行节点的classpath
        job.addCacheFile(new URI(args[2]));//缓存普通文件到运行节点的工作目录
        job.setNumReduceTasks(0);
        boolean res=job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
