package rcg.data;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;


public class EdgeWritable implements Writable {

	public long from;
	public long to;
	public int dist;
	public ArrayList<Long> vias = new ArrayList<Long>();
	public ArrayList<Integer> dets = new ArrayList<Integer>();
	
	
	public EdgeWritable() {
		this(0,0);
	}
	
	public EdgeWritable(long from, long to) {
		this.from = from;
		this.to = to;
	}


	public long id() {
		long s = from + to;
		return s*(s+1)/2 + from;
	}

	
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(from);
		out.writeLong(to);
		out.writeInt(dist);
		
		out.writeShort(vias.size());
		for (Long id : vias)
			out.writeLong(id);
		out.writeShort(dets.size());
		for (Integer id : dets)
			out.writeInt(id);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {

		from = in.readLong();
		to = in.readLong();
		dist = in.readInt();
		
		short deg = in.readShort();
		vias = new ArrayList<Long>(deg);
		for (int i=0; i<deg; i++)
			vias.add(in.readLong());
		deg = in.readShort();
		dets = new ArrayList<Integer>(deg);
		for (int i=0; i<deg; i++)
			dets.add(in.readInt());
	}


}
