package dedup;

import java.io.*;
import java.util.*;
import java.util.StringTokenizer;
import java.util.Random;
import java.math.BigInteger;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class Dedupmapred 
{

    public static class FMapper extends Mapper<Object, Text, IntWritable,Text>
    {
	private int numRecords = 0;
	String pat;
	String txt;
        long patHash;
	HTable table; 
	long txtHash;   
        int M;  
        long Q; 
        int R;   
        long RM;
        private static final LongWritable one = new LongWritable(1);
	private static final LongWritable zero = new LongWritable(0);
	public static final byte[] hash = "hash".getBytes();	
	String cmp;
	private Text text = new Text();
	public long hash(String key, int M,int start)
        { 
            long h = 0; 
            for (int j = start; j < M; j++) 
                h = (R * h + key.charAt(j)) % Q; 
            return h; 
        } 
	private static Put resultToPut(long key,String sub) throws IOException 
	{
  		Put put = new Put(Bytes.toBytes(String.valueOf(key)));
 		put.add(Bytes.toBytes("hash"), null, Bytes.toBytes(sub));
		return put;
	}

	public void map(Object row,Text value, Context context) throws IOException 
	{
		Configuration conf = context.getConfiguration();
           	pat= conf.get("pat");
		table = new HTable(conf, "hbase5mb" );
		String txt=value.toString();
		R = 256;
           	M = Integer.parseInt(pat);
           	Q = 99999991;
    //precompute R^(M-1) % Q for use in removing leading digit 
           	RM = 1;
	       for (int i = 1; i <= M-1; i++)
               		RM = (R * RM) % Q;
           	//patHash = hash(pat, M,0);
           	int N = txt.length(); 
            	String sub;
            	int s=0;
	     	//patHash = hash(pat, M,0);
		if(M<N)
		{
			sub=txt.substring(0,M);
            		long txtHash = hash(txt, M,value.charAt(0));
		}
		else
		{
			long txtHash = hash(txt, N,value.charAt(0));
			sub=txt.substring(0,N);
			try
			{
				table.put(resultToPut(txtHash,sub));
			}
			catch(Exception e)
			{
			}
		}
		
		 for (int i = M; i < N; i++)

		{
			
			try
			{
				table.put(resultToPut(txtHash,sub));
				context.write(new IntWritable(M),new Text("Done"));
			}
			catch(Exception f)
			{
					
			}
		
            		txtHash = (txtHash + Q - RM*txt.charAt(i - M) % Q) % Q; 
               		txtHash = (txtHash * R + txt.charAt(i)) % Q;
               		if(i+M<=N)
               		sub=txt.substring(i,i+M);
                	else
                	sub=txt.substring(i,N);
		}
	
            numRecords++;
            if ((numRecords % 100000) == 0) {
                context.setStatus("mapper processed " + numRecords + " records so far");
            }
        }
    }
   
   public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) 
        {
            System.err.println("Usage:dedup <in> <out> <pattern>");
            System.exit(2);
        }
	   String p=args[2];
	   conf.set("pat",p);
	   Configuration conf1 = HBaseConfiguration.create(conf);
	
        Job job1 = new Job(conf1, "Dedup");
        job1.setJarByClass(Dedupmapred.class);
        job1.setMapperClass(FMapper.class);
        job1.setNumReduceTasks(0);
        //job1.setCombinerClass(AReducer.class);
	job1.setMapOutputKeyClass(IntWritable.class);
	job1.setMapOutputValueClass(Text.class);
        //job1.setReducerClass(AReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
	/*MultipleInputs.addInputPath(job1,new Path(otherArgs[0]),  FileInputFormat.class, FMapper.class);
	MultipleInputs.addInputPath(job1,new Path(otherArgs[1]),  FileInputFormat.class, LMapper.class);*/
	FileInputFormat.addInputPath(job1, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
	long start = new Date().getTime();
   	boolean status = job1.waitForCompletion(true);            
   	long end = new Date().getTime();
   	System.out.println("Job took "+(end-start) + "milliseconds");
	System.exit(job1.waitForCompletion(true)? 0 : 1);
}
}
