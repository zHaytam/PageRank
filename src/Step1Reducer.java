import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Step1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> 
{

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
		String outlinks = "";
		boolean first = true;
		
		for (IntWritable iw : values)
		{
			if (first) first = false;
			else outlinks += ",";
			outlinks += Integer.toString(iw.get());
		}
		
		context.write(key, new Text("1.0\t" + outlinks));
	}
	
}