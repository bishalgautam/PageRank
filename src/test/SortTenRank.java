package test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SortTenRank {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		Text outKey =  new Text();
		Text outValue = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String readline = value.toString(); 
			String[] line = readline.split("\\s+");
			outValue.set(line[0]);
			double newKey = 10.0 - Double.parseDouble(line[1]);
			//System.out.println("bkey"+newKey);
			outKey.set(String.valueOf(newKey));
			context.write(outKey, outValue);
			}
		}	
	public static class Reduce extends Reducer<Text,Text, Text, Text>{
		Text outKey =  new Text();
		Text outValue = new Text();
		int count = 0;
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = value.iterator();
			while(it.hasNext()){
				
				double theKey = Math.round( (10.0 - Double.parseDouble(key.toString()))* 1000.0 ) / 1000.0;
				outKey.set(String.valueOf(theKey));
				if(count < 10){
					context.write(outKey, it.next());
					count++;
				}else
					break;
				
				//context.write(key, it.next());
			}
				
		}
	}
}
