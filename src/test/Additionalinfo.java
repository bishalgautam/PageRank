package test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
 

public class Additionalinfo {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		Text outKey =  new Text();
		Text outValue = new Text();
		int degree = 0;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String readline = value.toString(); 
			String[] line = readline.split("\\s+");
		
			degree = line.length - 1 ;
			outValue.set(String.valueOf(degree));
			outKey.set("indentifier");
			context.write(outKey, outValue);
			}
		}	
	public static class Reduce extends Reducer<Text,Text, Text, Text>{
		Text outKey =  new Text();
		Text outValue = new Text();
		int nodeCount= 0;
		int edgeCount = 0;
		int maxDegree = Integer.MIN_VALUE;
		int minDegree = Integer.MAX_VALUE;
		int degree = 0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = value.iterator();
			
			while(it.hasNext()){
				String read = it.next().toString();
				String[] reads = read.split("\\s+");
				degree = Integer.parseInt(reads[0]);
				if (degree > maxDegree)
					maxDegree = degree;
				else if(degree < minDegree)
					minDegree = degree;
				
				edgeCount += degree;
				nodeCount++;

		}
				outKey.set("Results:");
				outValue.set("\n Total no of nodes: "+ nodeCount+"\n Total no of Edges: "+ edgeCount+ "\n Maximun Degree: "+
				maxDegree+"\n Minimum Degree: "+ minDegree+"\n Average Degree: "+(edgeCount/nodeCount));
//				if(key.equals("identifier"))
				context.write(outKey, outValue);
		}
	}

}
