package test;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.Tool;


public class Pagerank{
	
//	long startTime = System.nanoTime();    
//	// ... the code being measured ...    
//	long estimatedTime = System.nanoTime() - startTime;
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		Text outKey =  new Text();
		Text outValue = new Text();
		Text link = new Text();
		
		double rank = 0.0;
		int degree = 0;
		
		// Counter
		public enum MyCounter {
			COUNTER;
		}
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String readline = value.toString(); 
			String[] nodeids = readline.split("\\s+");
			
			link.set(nodeids[0]);
			
			if(nodeids.length>=2){
					if(nodeids[1].length() < 6){
						rank = Double.parseDouble(nodeids[1]);
						degree = nodeids.length - 2;
						String adlist="";
						
						if(nodeids.length >=3){
							for(int i =2; i< nodeids.length; i++){
								adlist+= nodeids[i]+" ";
							}
						}
						outValue.set("value "+ String.valueOf(degree)+" "+adlist);
						context.write(link, outValue);
						
						outValue.set(link.toString()+" "+String.valueOf(rank)+" "+String.valueOf(degree));
						
						if(nodeids.length >= 3){
							for(int i=2; i< nodeids.length; i++){
								 outKey.set(nodeids[i]);
								 context.write(outKey, outValue);
						 }
			
					   }
					}else{			
						rank = 1.0;
						degree = nodeids.length -1;
						
						String adlist = "";
						if(nodeids.length >=2){
							 for(int i=1; i< nodeids.length; i++){
								 adlist+= nodeids[i]+ " ";
							 }
						}
						outValue.set("value "+ String.valueOf(degree)+" "+adlist);
						context.write(link, outValue);
						
						outValue.set(link.toString()+" "+String.valueOf(rank)+" "+String.valueOf(degree));
						
						for(int i=1; i< nodeids.length; i++){
							 outKey.set(nodeids[i]);
							 context.write(outKey, outValue);
						 }
						
					}
				
			}else{
				rank = 1.0;
				degree = 0;
				outValue.set("value 0");
				context.write(link, outValue); 
			}
			
			
			
		}
	}		
		public static class Reduce extends Reducer<Text,Text, Text, Text>{
			@Override
			protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
//				super.reduce(key, value, context);
//			}
			Iterator<Text> it = value.iterator();
			//it.next();
			double dfactor = 0.85;
			Text outVal = new Text();
			
//			public void reduce(Text key, Iterator<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException{
//				super.reduce(key, value, context);
				double total = 0.00;
				String outlinks= "";
				
				while(it.hasNext()){
//					it.next();
					String read = it.next().toString();
					String[] reads = read.split("\\s+");
					
					if(!reads[0].equals("value")){
						total+= Double.valueOf(reads[1]) / Double.valueOf(reads[2]);
					}else {
						if(reads.length > 2 ){
							for(int i=2; i< reads.length; i++){
								outlinks += reads[i]+ " ";
							}
						}
					}
				}
				total = dfactor * total + 1-dfactor;
				total = Math.round(total*1000.0)/1000.0;
				outVal.set(String.valueOf(total)+" "+ outlinks);
				context.write(key, outVal);
			}	
			
		}
		
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
			int i = 0;
			Path inpath = null;
			Path outpath = null;
			int iterations = Integer.parseInt(args[2]);
			
		while(i < iterations){
			inpath = (i==0) ? (new Path(args[0])) : (new Path(args[1]+String.valueOf(i-1)));
			outpath =  new Path((args[1])+String.valueOf(i)) ;
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "mapreduceiterative");
	       // job.addCacheFile(new Path(args[]).toUri());
	        job.setJarByClass(Pagerank.class);
	        job.setMapperClass(Map.class);
	        //job.setCombinerClass(Reduce.class);
	        job.setReducerClass(Reduce.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job, inpath);
	        FileOutputFormat.setOutputPath(job, outpath);
	        i++;
	        job.waitForCompletion(true);
//	        if(i > 1){
//	        	FileSystem fs = FileSystem.get(conf);
//	        	fs.delete(new Path(args[1]+String.valueOf(i-2)),true);	
//	        }
	       //System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
           
			// sorted pagerank  job starts here
			
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "sortpageRank");
	        job.setJarByClass(SortTenRank.class);
	        job.setMapperClass(SortTenRank.Map.class);
	        job.setReducerClass(SortTenRank.Reduce.class);
	        job.setNumReduceTasks(1); // gives one output file
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
//	        FileInputFormat.addInputPath(job, new Path(args[1]+String.valueOf(i-1)));
	        FileInputFormat.addInputPath(job, new Path(args[1]+(i-1)));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]+"sorted"));
	        job.waitForCompletion(true);
	        
	        // printing additional information 
	        
	        Configuration conf1 = new Configuration();
	        Job job2 = Job.getInstance(conf1, "Additionalinfo");
	        job2.setJarByClass(Additionalinfo.class);
	        job2.setMapperClass(Additionalinfo.Map.class);
	        job2.setReducerClass(Additionalinfo.Reduce.class);
	        job2.setNumReduceTasks(1); // gives one output file
	        job2.setOutputKeyClass(Text.class);
	        job2.setOutputValueClass(Text.class);
	        //FileInputFormat.addInputPath(job, new Path(args[1]+String.valueOf(i-1)));
	        FileInputFormat.addInputPath(job2, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"information"));
	        job2.waitForCompletion(true);
	        
			
		}

//		@Override
//		public Configuration getConf() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public void setConf(Configuration arg0) {
//			// TODO Auto-generated method stub
//			
//		}
//
//		@Override
//		public int run(String[] arg0) throws Exception {
//			// TODO Auto-generated method stub
//			return 0;
//		}
		
	
}
