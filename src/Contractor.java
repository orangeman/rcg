

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



public class Contractor extends Configured implements Tool {

    public static class OsmMapper extends Mapper<LongWritable, Writable, LongWritable, NodeWritable>{

        public void map(LongWritable key, Writable value, Context context) throws IOException, InterruptedException {

        	if (value instanceof NodeWritable) {
        		context.write(key, (NodeWritable) value);
        	} else {
        		Writable[] wayNodes = ((ArrayWritable)value).get();

        		for (int i = 1; i < wayNodes.length; i++) {
        			context.write((LongWritable) wayNodes[i-1], new NodeWritable(((LongWritable)wayNodes[i]).get()));
        			context.write((LongWritable) wayNodes[i], new NodeWritable(((LongWritable)wayNodes[i-1]).get()));
        		}
        	}

        }
    }

    public static class OsmReducer extends Reducer<LongWritable, NodeWritable, LongWritable, NodeWritable> {

        public void reduce(LongWritable key, Iterable<NodeWritable> values, Context context) throws IOException, InterruptedException {

//        	System.out.println("Reduce");
        	NodeWritable node = null;
        	Iterator<NodeWritable> iter = values.iterator();
        	ArrayList<Long> outgoing = new ArrayList<Long>();
        	while (iter.hasNext()) {
				NodeWritable val = (NodeWritable) iter.next();
				if (!val.lat.equals("") && !val.lon.equals("")) {
					node = new NodeWritable(key.get());
					node.id = key.get();
					node.lat = val.lat;
					node.lon = val.lon;
					node.outgoing = outgoing;
//					System.out.println("hier lat:"+node.lat+" - deg:"+outgoing.size());
				} else {
					outgoing.add(val.id);
//					System.out.println("add edge");
				}
			}
        	if (node != null && node.degree() > 0) {
        		context.write(key, node);
//        		System.out.println("out Node");
        	} else
        		System.out.println("dismiss");
        }
    }
    public static class KmlMapper extends Mapper<LongWritable, NodeWritable, LongWritable, NodeWritable>{
    	
    	public void map(LongWritable key, NodeWritable node, Context context) throws IOException, InterruptedException {
    		
    		context.write(key, node);
    		for (long to : node.outgoing) {
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
    			} else {
    				NodeWritable nn = new NodeWritable(n.id);
    				nn.lat = n.lat;
    				nn.lon = n.lon;
    				outgoing.add(nn);
    				}
    		}
    		
    		if (node != null) {
    		for (NodeWritable n : outgoing) {
    				context.write(n, node);
    		}
    		} else
    			System.out.println("node "+key.get()+" has not emitted itself");
    	}
    }
    
    



    public static void main(String[] args) throws Exception {
    	int result = ToolRunner.run(new Configuration(), new Contractor(), args);
    	System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
    	Configuration conf = getConf();
    	try {
    		FileSystem.get(conf).delete(new Path("nodes"));
    		FileSystem.get(conf).delete(new Path("output"));
    		
    		Job job = new Job(conf, "osm");
    		job.setMapperClass(OsmMapper.class);
    		job.setReducerClass(OsmReducer.class);
//    		job.setNumReduceTasks(0);
    		job.setOutputKeyClass(LongWritable.class);
    		job.setOutputValueClass(NodeWritable.class);
    		job.setOutputFormatClass(SequenceFileOutputFormat.class);
    		job.setInputFormatClass(OsmFileInputFormat.class);
    		FileInputFormat.setInputPaths(job, new Path("input"));
    		FileOutputFormat.setOutputPath(job, new Path("nodes"));
    		job.waitForCompletion(true);
    		
    		job = new Job(conf, "osm");
//    		job.setJarByClass(Contractor.class);
    		job.setMapperClass(KmlMapper.class);
    		job.setReducerClass(KmlReducer.class);
    		job.setMapOutputKeyClass(LongWritable.class);
    		job.setOutputKeyClass(NodeWritable.class);
    		job.setOutputValueClass(NodeWritable.class);
    		job.setOutputFormatClass(KmlFileOutputFormat.class);
    		job.setInputFormatClass(SequenceFileInputFormat.class);
    		FileInputFormat.setInputPaths(job, new Path("nodes"));
    		FileOutputFormat.setOutputPath(job, new Path("output"));
    		System.exit(job.waitForCompletion(true) ? 0 : 1);
    	} catch (IOException e) {
    		e.printStackTrace();
    		System.exit(3);
    	}
    	return 0;

    }

}
