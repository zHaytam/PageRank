import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> 
{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] split = value.toString().split("\t");
		String node = split[0];
		float rank = Float.parseFloat(split[1]);
		
		// If the node doesn't have outlinks
		if (split.length < 3)
			return;
		
		String[] outlinks = split[2].split(",");
		
		for (String outlink : outlinks)
		{
			float outlinkRank = rank / outlinks.length;
			context.write(new Text(outlink), new Text(Float.toString(outlinkRank)));
		}
		
		context.write(new Text(node), new Text("[" + split[2]));
	}
	
}
