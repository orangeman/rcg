package rcg.io;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import rcg.data.NodeWritable;


public class KmlFileOutputFormat extends FileOutputFormat<NodeWritable, NodeWritable> {

	
	@Override
	public RecordWriter<NodeWritable, NodeWritable> getRecordWriter(TaskAttemptContext ctx) throws IOException, InterruptedException {
		Path file = new Path(FileOutputFormat.getOutputPath(ctx).toString()+"/nodes.kml");
	    FileSystem fs = file.getFileSystem(ctx.getConfiguration());
	    FSDataOutputStream fileOut = fs.create(file);
		return new KmlRecordWriter(fileOut);
	}

	
	protected class KmlRecordWriter extends RecordWriter<NodeWritable, NodeWritable> {
		
		private DataOutputStream out;

		public KmlRecordWriter(DataOutputStream out) throws IOException {
			this.out = out;
			out.write(("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
						"<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" +
						"<Document>\n" +
						"<Style id=\"orangeLine\">" +
						"<LineStyle><color>44ffa500</color><width>42</width></LineStyle>" +
						"</Style>\n\n").getBytes());
		}
		
		@Override
		public void write(NodeWritable from, NodeWritable to) throws IOException,InterruptedException {
			out.write(("<Placemark>\n" +
			    "\t<name>"+from.id+"->"+to.id+"</name>\n" +
			    "<styleUrl>#orangeLine</styleUrl>" +
			    "\t<LineString><extrude>1</extrude><tessellate>1</tessellate>" +
			    "<coordinates>"+from.lon+","+from.lat+",0 "+to.lon+","+to.lat+",0"+
			    "</coordinates></LineString>\n" +
			    "</Placemark>\n").getBytes());
			out.write(("<Placemark>\n" +
				"\t<name>"+from.degree()+"-"+from.conn+"</name>\n" +
				"\t<description>" +from.distances.get(from.neighbours.indexOf(to.id))+"</description>\n" +
				"\t<Point><coordinates>"+from.lon+","+from.lat +
				"</coordinates></Point>\n" +
			"</Placemark>\n").getBytes());
			
		}
		
		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			out.write("</Document></kml>".getBytes());
			out.close();
		}

	}

}
