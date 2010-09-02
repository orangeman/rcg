package rcg.data;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;


public class NodeWritable implements Writable {

	public long id;
	public float lat;
	public float lon;
	public ArrayList<Long> neighbours;
	public ArrayList<Integer> distances;
	
	
	public NodeWritable() {
		this(0);
	}
	public NodeWritable(long id) {
		this.id = id;
		neighbours = new ArrayList<Long>();
		distances = new ArrayList<Integer>();
	}
	
	public int degree() {
		return neighbours.size();
	}

	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeFloat(lat);
		out.writeFloat(lon);
		
		out.writeInt(neighbours.size());
		for (Long id : neighbours)
			out.writeLong(id);
		out.writeInt(distances.size());
		for (Integer dist : distances)
			out.writeInt(dist);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readLong();
		lat = in.readFloat();
		lon = in.readFloat();

		distances.clear();
		neighbours.clear();
		int degree = in.readInt();
		for (int i=0; i<degree; i++)
			neighbours.add(in.readLong());
		degree = in.readInt();
		for (int i=0; i<degree; i++)
			distances.add(in.readInt());
	}


}
