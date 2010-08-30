package rcg.io;




import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import rcg.data.NodeWritable;

/**
 * MatchInputFormat is a {@link OsmRecordReader} only input format.
 */
public class OsmFileInputFormat extends FileInputFormat<LongWritable, Writable> {

	@Override
	public RecordReader<LongWritable, Writable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new OsmRecordReader();
	}

//	@Override
//	protected boolean isSplitable(JobContext job, Path path) {
//		final CompressionCodec codec = new CompressionCodecFactory(
//		job.getConfiguration()).getCodec(path);
//		return (codec == null) || (codec instanceof SplitEnabledCompressionCodec);
//		return true;
//	}

	protected class OsmRecordReader extends RecordReader<LongWritable, Writable> {

		private Scanner scanner;
		private FSDataInputStream dataIn;
		
		private LongWritable mKey;
		private Writable currentValue;
		private ArrayList<Long> wayNodes;
		private NodeWritable mNode;
		private ArrayList<LongWritable> mWayNodes;
		private ArrayWritable mWay;


		
		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext ctx) throws IOException, InterruptedException {

			ctx.setStatus("Running");

			FileSplit split = (FileSplit) genericSplit;
			FileSystem fs = split.getPath().getFileSystem(ctx.getConfiguration());
			dataIn = fs.open(split.getPath());
			dataIn.seek(split.getStart());

			scanner = new Scanner(dataIn);
			mKey = new LongWritable();
			mNode = new NodeWritable(0);
			mWayNodes = new ArrayList<LongWritable>();
			mWay = new ArrayWritable(LongWritable.class);
		}
		

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {

			while (scanner.findWithinHorizon("(<node|<way) id='(\\d*)'", 10000) != null) {
				if (scanner.match().group(1).equals("<node")) {
					mKey.set(Long.valueOf(scanner.match().group(2)));
					scanner.findWithinHorizon("lat='(.*)' lon='(.*)'", 300);
					mNode.lat = scanner.match().group(1);
					mNode.lon = scanner.match().group(2);
					mNode.id = 0;
					currentValue = mNode;
					return true;
				} else if (scanner.match().group(1).equals("<way")){
					scanner.findWithinHorizon(">", 100);
					mWayNodes.clear();
					while (scanner.findWithinHorizon("<nd ref='(\\d*)' />", 30) != null) {
						mWayNodes.add(new LongWritable(new Long(scanner.match().group(1))));
					}
					while (scanner.findWithinHorizon("<tag", 10) != null) {
						scanner.findWithinHorizon("k='(.*)' v='(.*)' />", 100);
						if (scanner.match().group(1).equals("highway") && mWayNodes.size() > 0) {
							mWay.set((Writable[]) mWayNodes.toArray(new LongWritable[mWayNodes.size()]));
							currentValue = mWay;
							return true;
						}
					}
				}
			}
			System.out.println("hm");
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return mKey;
		}

		@Override
		public Writable getCurrentValue() throws IOException, InterruptedException {
			return currentValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void close() throws IOException {
			dataIn.close();
			mKey = null;
			currentValue = null;
		}



	}

	
}
