package rcg;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import rcg.data.NodeWritable;
import rcg.io.KmlFileOutputFormat;



public class ExportKml extends Configured implements Tool {

	
    public static class KmlMapper extends Mapper<LongWritable, NodeWritable, LongWritable, NodeWritable>{
    	
    	public void map(LongWritable key, NodeWritable node, Context context) throws IOException, InterruptedException {
    		
    		context.write(key, node);
    		for (long to : node.neighbours) {
    			context.write(new LongWritable(to), node);
    		}
    		
    	}
    }
    
    
    public static class KmlReducer extends Reducer<LongWritable, NodeWritable, NodeWritable, NodeWritable> {
    	
    	public void reduce(LongWritable key, Iterable<NodeWritable> values, Context context) throws IOException, InterruptedException {
    		
    		NodeWritable node = null;
    		ArrayList<NodeWritable> outgoing = new ArrayList<NodeWritable>();
    		for (NodeWritable n : values) {
    			if (n.id == key.get()) {
    				node = new NodeWritable(n.id);
    				node.lat = n.lat;
    				node.lon = n.lon;
    				node.neighbours = (ArrayList<Long>) n.neighbours.clone();
    				node.distances = (ArrayList<Integer>) n.distances.clone();
    			} else {
    				NodeWritable nn = new NodeWritable(n.id);
    				nn.lat = n.lat;
    				nn.lon = n.lon;
    				nn.neighbours = (ArrayList<Long>) n.neighbours.clone();
    				nn.distances = (ArrayList<Integer>) n.distances.clone();
    				outgoing.add(nn);
    				}
    		}
    		
    		if (node != null) {
    		for (NodeWritable n : outgoing) {
    				context.write(n, node);
    		}
    		} 
    		else System.out.println("node "+key.get()+" has not emitted itself");
    	}
    }
    

    @Override
    public int run(String[] args) throws Exception {

    	try {
    		
    		Path in = new Path(args[0]);
    		Path out = new Path(args[1]);
    		Job myJob = new Job(getConf(), "");
//    		myJob.setJarByClass(ExportKml.class);
    		myJob.setMapperClass(KmlMapper.class);
    		myJob.setReducerClass(KmlReducer.class);
    		FileInputFormat.setInputPaths(myJob, in);
    		FileOutputFormat.setOutputPath(myJob, out);
    		myJob.setMapOutputKeyClass(LongWritable.class);
    		myJob.setOutputKeyClass(NodeWritable.class);
    		myJob.setOutputValueClass(NodeWritable.class);
    		myJob.setOutputFormatClass(KmlFileOutputFormat.class);
    		myJob.setInputFormatClass(SequenceFileInputFormat.class);
    		
    		System.exit(myJob.waitForCompletion(true) ? 0 : 1);
    	} catch (IOException e) {
    		e.printStackTrace();
    		System.exit(3);
    	}
    	return 0;
    }

    
    
    
    
    public static void main(String[] args) throws Exception {
    	int result = ToolRunner.run(new Configuration(), new ExportKml(), args);
    	System.exit(result);
    }
}
