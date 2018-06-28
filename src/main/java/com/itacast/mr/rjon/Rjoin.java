package com.itacast.mr.rjon;


import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;



import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.security.krb5.Config;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class Rjoin {
    static class  RjopnMapper extends Mapper<LongWritable,Text,Text,InforBean>{
        InforBean bean =new InforBean();
        Text kText=new Text();
        @Override

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            FileSplit inputSplit = (FileSplit) context.getInputSplit();//获得切片而获得文件信息
            String name=inputSplit.getPath().getName();
            String pid="";
            if(name.startsWith("order")){
                    String[] field=line.split(",");
                pid=field[2];
                    bean.set(Integer.parseInt(field[0]),field[1],pid,Integer.parseInt(field[3]),"",0,0,"0");

            }else{
                String[] field=line.split(",");
                pid=field[0];
                bean.set(0,"",pid,0,field[1],Integer.parseInt(field[2]),Integer.parseInt(field[3]),"1");
            }
                kText.set(pid);
                context.write(kText,bean);

        }
    }
    static  class RjonReducer extends Reducer<Text,InforBean,InforBean,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<InforBean> beans, Context context) throws IOException, InterruptedException {
            InforBean pdBean=new InforBean();
            ArrayList<InforBean> inforBeans = new ArrayList<InforBean>();
            for (InforBean bean:beans){
                if("1".equals(bean.getFlag())){
                    try {
                        BeanUtils.copyProperties(pdBean,bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                else{
                    InforBean odBean=new InforBean();
                    try {
                        BeanUtils.copyProperties(odBean,bean);
                        inforBeans.add(odBean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
            for (InforBean bean:inforBeans){
                bean.setPname(pdBean.getPname());
                bean.setCategory_id(pdBean.getCategory_id());
                bean.setPrice(pdBean.getPrice());
                context.write(bean,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf =new Configuration();
        Job job=Job.getInstance(conf);
        job.setJarByClass(Rjoin.class);
        job.setMapperClass(RjopnMapper.class);
        job.setReducerClass(RjonReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InforBean.class);
        job.setOutputKeyClass(InforBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
       boolean res= job.waitForCompletion(true);
       System.exit(res?0:1);
    }
}
