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

import rcg.ImportOsm.DistMapper;
import rcg.ImportOsm.DistReducer;
import rcg.ImportOsm.OsmMapper;
import rcg.ImportOsm.OsmReducer;
import rcg.data.EdgeWritable;
import rcg.data.NodeWritable;
import rcg.io.KmlFileOutputFormat;



public class Contract extends Configured implements Tool {

	
    public static class ExpandMapper extends Mapper<LongWritable, NodeWritable, LongWritable, EdgeWritable>{
    	
    	public void map(LongWritable key, NodeWritable node, Context context) throws IOException, InterruptedException {
    		
    		for (int i = 0; i < node.neighbours.size(); i++) {
    			EdgeWritable edge = new EdgeWritable(node.id, node.neighbours.get(i));
    			edge.dist = node.distances.get(i);
    			context.write(new LongWritable(edge.id()), edge);
    			for (int j = 0; j < node.neighbours.size(); j++) {
    				if (i != j) {
    					EdgeWritable edge2 = new EdgeWritable(node.neighbours.get(i), node.neighbours.get(j));
    					edge2.dist = node.distances.get(i) + node.distances.get(j);
    					edge2.vias.add(key.get());
    					context.write(new LongWritable(edge2.id()), edge2);
    				}
    			}
			}
    	}
    }
    
    
    public static class MinDistReducer extends Reducer<LongWritable, EdgeWritable, LongWritable, EdgeWritable> {
    	
    	public void reduce(LongWritable key, Iterable<EdgeWritable> values, Context context) throws IOException, InterruptedException {
    		int minDist = Integer.MAX_VALUE;
    		EdgeWritable edge = new EdgeWritable(0, 0);
    		for (EdgeWritable e : values) {
    			if (!e.vias.isEmpty()) {
    				edge.vias.add(e.vias.get(0));
    				edge.dets.add(e.dist);
    			}
    			if (e.dist < minDist) {
    				minDist = e.dist;
    				edge.from = e.from;
    				edge.to = e.to;
    			}
    		}
    		edge.dist = minDist;
    		for (int i=0; i<edge.dets.size(); i++) {
    			edge.dets.set(i, edge.dets.get(i) - minDist);
    		}
    		context.write(new LongWritable(edge.id()), edge);
    	}
    }
    
    
    
    
    public static class ContractMapper extends Mapper<LongWritable, EdgeWritable, LongWritable, EdgeWritable>{
    	
    	public void map(LongWritable key, EdgeWritable edge, Context context) throws IOException, InterruptedException {
    		
    		context.write(new LongWritable(edge.from), edge);
    		for (long id : edge.vias)
    			context.write(new LongWritable(id), new EdgeWritable());
//    		context.write(new LongWritable(edge.to), edge);
    		
    	}
    }
    
    
    public static class JoinReducer extends Reducer<LongWritable, EdgeWritable, LongWritable, NodeWritable> {
    	
    	NodeWritable myNode;

		public void reduce(LongWritable key, Iterable<EdgeWritable> edges, Context context) throws IOException, InterruptedException {
			
			int via_counter = 0;
			myNode = new NodeWritable(key.get());
			for (EdgeWritable e : edges) {
				if (e.from == 0)
					via_counter++;
				if (!e.vias.isEmpty()) {
					if (e.to == myNode.id)
						myNode.neighbours.add(e.from);
					else
						myNode.neighbours.add(e.to);
					myNode.distances.add(e.dist);
				}
			}
			myNode.conn = via_counter;
			context.write(key, myNode);
    	}
    }
    

    @Override
    public int run(String[] args) throws Exception {

    	try {
    		Path in = new Path(args[0]);
    		Path out = new Path("intermediate");
    		Job myJob = new Job(getConf(), "");
    		myJob.setMapperClass(ExpandMapper.class);
    		myJob.setReducerClass(MinDistReducer.class);
    		FileInputFormat.setInputPaths(myJob, in);
    		FileOutputFormat.setOutputPath(myJob, out);
    		myJob.setMapOutputKeyClass(LongWritable.class);
    		myJob.setOutputValueClass(EdgeWritable.class);
    		myJob.setInputFormatClass(SequenceFileInputFormat.class);
    		myJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    		
    		myJob.waitForCompletion(true);
    		
    		in = out;
    		out = new Path(args[1]);
    		myJob = new Job(getConf(), "");
//    		myJob.setJarByClass(ExportKml.class);
    		myJob.setMapperClass(ContractMapper.class);
    		myJob.setReducerClass(JoinReducer.class);
    		FileInputFormat.setInputPaths(myJob, in);
    		FileOutputFormat.setOutputPath(myJob, out);
    		myJob.setOutputKeyClass(LongWritable.class);
    		myJob.setMapOutputValueClass(EdgeWritable.class);
    		myJob.setOutputValueClass(NodeWritable.class);
    		myJob.setInputFormatClass(SequenceFileInputFormat.class);
    		myJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    		
    		myJob.waitForCompletion(true);
    		
    		FileSystem.get(getConf()).delete(new Path("intermediate"));
    	} catch (IOException e) {
    		e.printStackTrace();
    		System.exit(3);
    	}
    	return 0;
    }

    
    
    
    
    public static void main(String[] args) throws Exception {
    	int result = ToolRunner.run(new Configuration(), new Contract(), args);
    	System.exit(result);
    }
}
