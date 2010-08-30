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
import rcg.io.OsmFileInputFormat;



public class ImportOsm extends Configured implements Tool {

	
	
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
				} else {
					outgoing.add(val.id);
				}
			}
        	if (node != null && node.degree() > 0) {
        		context.write(key, node);
        	} 
//        	else System.out.println("dismiss");
        }
    }

    
    @Override
    public int run(String[] args) throws Exception {
    		
    	try {
    		
    		Path in = new Path(args[0]);
    		Path out = new Path(args[1]);
    		Job myJob = new Job(getConf(), "");
//    		myJob.setJarByClass(ImportOsm.class);
    		myJob.setMapperClass(OsmMapper.class);
    		myJob.setReducerClass(OsmReducer.class);
    		FileInputFormat.setInputPaths(myJob, in);
    		FileOutputFormat.setOutputPath(myJob, out);
    		myJob.setOutputKeyClass(LongWritable.class);
    		myJob.setOutputValueClass(NodeWritable.class);
    		myJob.setInputFormatClass(OsmFileInputFormat.class);
    		myJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    		
    		myJob.waitForCompletion(true);
    		
    	} catch (IOException e) {
    		e.printStackTrace();
    		System.exit(3);
    	}
    	return 0;
    }

    
    
    public static void main(String[] args) throws Exception {
    	int result = ToolRunner.run(new Configuration(), new ImportOsm(), args);
    	System.exit(result);
    }
}
