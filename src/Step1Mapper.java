import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Step1Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
{

	private static int nodesCount;
	private static int currentNode;
	private static int edgesCount;
	private static int currentEdge;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		// NodesCount(N) EdgesCount(M)
		if (key.get() == 0) 
		{
			String[] split = value.toString().split(" ");
			nodesCount = Integer.parseInt(split[0]);
			edgesCount = Integer.parseInt(split[1]);
		}
		else
		{
			// N nodes
			if (currentNode < nodesCount)
			{
				currentNode++;
				return;
			}
			// M edges
			else if (currentEdge < edgesCount)
			{
				String[] split = value.toString().split(" ");
				context.write(new IntWritable(Integer.parseInt(split[0])), new IntWritable(Integer.parseInt(split[1])));
				currentEdge++;
			}
		}
		
	}
	
}