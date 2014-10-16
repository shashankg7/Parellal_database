package org.myorg.DistributedDatabase;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;


import org.apache.hadoop.conf.*;
import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.cli.*;
import org.apache.hadoop.mapred.JobClient;

import org.apache.hadoop.mapred.JobConf;

public class DistributedDatabase {
	private static int row_no=1;
	private static Hashtable<String,Integer> hash= new Hashtable<String,Integer>();
	public static class query_data
	{
		private String where_field;
		private String operator;
		private int value;
		private String str_value;
		private int [] select_col = new int[5];
		

		public query_data(int ind0,int ind1,int ind2,int ind3,int ind4,String where_field,String operator,String str,int value)
	{
			//this.select_col=select_tuples;
	select_col[0]=ind0;
	select_col[1]=ind1;
	select_col[2]=ind2;
	select_col[3]=ind3;
	select_col[4]=ind4;
			this.where_field=where_field;
			this.operator=operator;
			this.value=value;
		
			this.str_value=str;
		}
	}
	 class key_struct{
  	private IntWritable rollNo,colNo;
                public  key_struct(IntWritable r_no, IntWritable col_no)
                {
                        this.rollNo = r_no;
                        this.colNo=col_no;
                }
}

	public static class QueryMapper extends Mapper<LongWritable ,Text , LongWritable,Text>
	{
		// To get config info(parsed info) from main function
		
				//this is the current column number processed by mapper(Assuming it processes the line sequentially)
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException 
		{
		int col_no=0,col_no1=0;
		Configuration conf = context.getConfiguration();
		Text word = new Text();
		Text word1 = new Text();
		int m;
		
		m = Integer.parseInt(conf.get("int_value"));
		

	
		int m0 = Integer.parseInt(conf.get("id"));
		int m1 = Integer.parseInt(conf.get("name"));
		int m2 = Integer.parseInt(conf.get("branch"));
		int m3 = Integer.parseInt(conf.get("subject"));
		int m4 = Integer.parseInt(conf.get("marks"));
		query_data data=new query_data(0,m1,m2,m3,m4,conf.get("where_f"),conf.get("operator"),conf.get("str_value"),m);




		String line = value.toString();
		String val="";
		int flag=0;
		StringTokenizer tokenizer = new StringTokenizer(line);
		StringTokenizer tokenizer1 = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens())
			{
			
			 word.set(tokenizer.nextToken());
			
			if(conf.get("where_f").equals("id"))
			m0=0;	
			else if(conf.get("where_f").equals("name"))
			m0=1;
			else if(conf.get("where_f").equals("branch"))
			m0=2;
			else if(conf.get("where_f").equals("subject"))
			m0=3;
			else if(conf.get("where_f").equals("marks"))
			m0=4;
			while(tokenizer1.hasMoreTokens())
			{
				word1.set(tokenizer1.nextToken());
				if(col_no1==m0 && conf.get("where_f").equals("marks"))
				{
					if(data.operator.equals("="))
					{
						if(data.value==Integer.parseInt(word1.toString()))
						flag=1;
					}
				}
				else
				{
					if(data.operator.equals("&"))
					{
						if(data.str_value.equals(word1.toString()))
						flag=1;
					}	
				}
				col_no1++;
			}
			if(data.select_col[col_no]==1)
				{
				LongWritable in=new LongWritable(row_no);
				if(flag==1)			
				val=val+word.toString()+" ";

				}
				col_no++;
			}
		//val+=m0+" "+conf.get("where_f")+" "+conf.get("operator")+" "+data.value;
		if(flag==1)
		context.write(new LongWritable(row_no),new Text(val));
		row_no++;
		}
}
public static class IntSumReducer extends Reducer<LongWritable,Text, LongWritable,Text> 
{
   public void reduce(LongWritable key,Text value,  Context context) throws IOException, InterruptedException 
{
      LongWritable v=new LongWritable(21);
      context.write(key,value);
}
}
 public static void main(String[] args) throws Exception {

    Configuration conf1 = new Configuration();
	
  //  String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
	   		// column index of columns in the table
	long start = new Date().getTime();
    Configuration conf = new Configuration();
        // A is an m-by-n matrix; B is an n-by-p matrix.
	String Str=args[1];
	String[] col_names = new String[5];   //max 5 col can be displayed
	int i=0,col_no=0,j=0,no_proc; 
	String table_name="";
	String key="",value="",op="";

	String[]  s= new String[2];
	boolean b  = Str.startsWith("select");

	if(b){
		String[] retval = Str.split("from", 2);
		System.out.println(retval[0]);
		System.out.println(retval[1]);
		String before_from = retval[0];
		String after_from = retval[1];
		String[] col_part = before_from.split("select",2);

		for(String val : col_part[1].split(",",5))
		{
			col_names[i] = val.trim();
//			System.out.println(col_names[i]);
			
			i++;	
		}	
			col_no = i;  // total no of col in query
			String[] ret=after_from.split("where",2);
			table_name=ret[0].trim();
			
//			System.out.println(table_name);	
						
			int p = ret[1].indexOf('=');			
			

			if(p!=-1)
			{
				s = ret[1].split("=",2);				
				key = s[0].trim();
				value = s[1].trim();
				op = "=";
				
			}
			else{
			  	s = ret[1].split("&",2);
				if(!s[1].equals(""))
				{
					key = s[0].trim();
					value = s[1].trim();
					op="&";

				}
			}
		
		} // boolean if ending
	

	conf1.set("int_value","0");
	conf1.set("str_value","");
	 conf1.set("id","0");
    	conf1.set("name","0");
    	conf1.set("branch","0");
    	conf1.set("subject","0");
    	conf1.set("marks","0");

		String path="/shashank/";
	for(j=0;j<col_no;j++)
	{		
		conf1.set(col_names[j],"1");
	
	}		
		
 
    conf1.set("where_f", key);
    conf1.set("operator", op);
	if(key.equals("marks"))
    conf1.set("int_value", value);
	else
	 conf1.set("str_value", value);
// to know input file size  
	String str;
	FileSystem hdfs =FileSystem.get(conf1);
	str=conf1.get("fs.default.name");
	str =str+path+table_name;
	System.out.println(str);
	Path pr = new Path(str);
	FileStatus f = hdfs.getFileStatus(pr);
	System.out.println("len of file :" +f.getLen());
  
	long file_size,ans,count=0,split;
	file_size = f.getLen();
		//System.out.println("split " + split);
	/** To Set block size for partitionj model****/
	/*while((file_size)>0)
	{
		
		System.out.println("len of file :"+file_size);
		file_size=file_size/65536;
		count++;
		
		
	}
//	file_size *= 65536;
	split = file_size/no_proc;
	//conf1.setLong( "dfs.block.size",split) ;
    //conf1.set("string_value",args[4]);
    /*
   Job job = new Job(conf1,"DistributedDatabase");
   JobConf job = new JobConf(conf1, DistributedDatabase.class);
    job.setJarByClass(DistributedDatabase.class);
    job.setMapperClass(QueryMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
           job.setInputFormatClass(FileInputFormat.class);
        job.setOutputFormatClass(FileOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(path));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 	  job.waitForCompletion(true);*/
 	  

    
    Job job = new Job(conf1, "Distributed Database");
    job.setJarByClass(DistributedDatabase.class);
    job.setMapperClass(QueryMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(path));
    FileOutputFormat.setOutputPath(job, new Path(args[0]));
    System.out.println("path :" +args[0]);
    boolean status = job.waitForCompletion(true);            
   long end = new Date().getTime();
   System.out.println("Job took "+(end-start) + "milliseconds");
   
    System.exit(status ? 0 : 1);
 	  	 	     
 	   }
	}	
