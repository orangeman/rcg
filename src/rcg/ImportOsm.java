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

import rcg.ExportKml.KmlMapper;
import rcg.ExportKml.KmlReducer;
import rcg.data.NodeWritable;
import rcg.io.KmlFileOutputFormat;
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
        	ArrayList<Long> neighbours = new ArrayList<Long>();
        	while (iter.hasNext()) {
				NodeWritable val = (NodeWritable) iter.next();
				if (val.id == 0) {
					node = new NodeWritable(key.get());
					node.id = key.get();
					node.lat = val.lat;
					node.lon = val.lon;
					node.neighbours = neighbours;
				} else {
					neighbours.add(val.id);
				}
			}
        	if (node != null && node.degree() > 0) {
        		context.write(key, node);
        	} 
//        	else System.out.println("dismiss");
        }
    }

    
    
    public static class DistMapper extends Mapper<LongWritable, NodeWritable, LongWritable, NodeWritable>{
    	
    	public void map(LongWritable key, NodeWritable node, Context context) throws IOException, InterruptedException {
    		
    		context.write(key, node);
    		for (long nb_id : node.neighbours) {
    			context.write(new LongWritable(nb_id), node);
    		}
    		
    	}
    }
    
    
    public static class DistReducer extends Reducer<LongWritable, NodeWritable, LongWritable, NodeWritable> {
    	
    	NodeWritable node;
    	ArrayList<Long> neighbours;
    	ArrayList<Integer> distances;
    	
    	
    	@Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
			neighbours = new ArrayList<Long>();
			distances = new ArrayList<Integer>();
		}

		public void reduce(LongWritable key, Iterable<NodeWritable> nodes, Context context) throws IOException, InterruptedException {
			
			node = null;
			distances.clear();
    		neighbours.clear();
    		for (NodeWritable n : nodes) {
    			if (n.id == key.get()) {
    				node = new NodeWritable(n.id);
    				node.lat = n.lat;
    				node.lon = n.lon;
    			} else {
    				neighbours.add(n.id);
    				if (node != null)
    					distances.add(distance(node.lat, node.lon, n.lat, n.lon));
    				else
    					distances.add(0);
    			}
    		}
    		node.neighbours = neighbours;
    		node.distances = distances;
    		context.write(key, node);
    		
//    		System.out.println("node "+key.get()+" has not emitted itself");
    	}
    }
    
    
    static Integer distance(float lat1, float lon1, float lat2, float lon2) {
        double earthRadius = 3958.75;
        double dLat = Math.toRadians(lat2-lat1);
        double dLng = Math.toRadians(lon2-lon1);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLng/2) * Math.sin(dLng/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        double dist = earthRadius * c;

        int meterConversion = 1609;

        return new Integer((int)(dist * meterConversion));
    }

    
    
    @Override
    public int run(String[] args) throws Exception {
    		
    	try {
    		
    		Path in = new Path(args[0]);
    		Path out = new Path("intermediate");
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
    		
    		in = out;
    		out = new Path(args[1]);
    		myJob = new Job(getConf(), "");
//    		myJob.setJarByClass(ExportKml.class);
    		myJob.setMapperClass(DistMapper.class);
    		myJob.setReducerClass(DistReducer.class);
    		FileInputFormat.setInputPaths(myJob, in);
    		FileOutputFormat.setOutputPath(myJob, out);
    		myJob.setOutputKeyClass(LongWritable.class);
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
    	int result = ToolRunner.run(new Configuration(), new ImportOsm(), args);
    	System.exit(result);
    }
}
