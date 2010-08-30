import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;


public class NodeWritable implements Writable {

	long id;
	String lat = "";
	String lon = "";
	ArrayList<Long> outgoing;
	
	
	public NodeWritable() {
		this.id = 0;
		outgoing = new ArrayList<Long>();
	}
	public NodeWritable(long id) {
		this.id = id;
		outgoing = new ArrayList<Long>();
	}
	
	public int degree() {
		return outgoing.size();
	}

	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeUTF(lat);
		out.writeUTF(lon);
		
		out.writeInt(outgoing.size());
		for (Long id : outgoing)
			out.writeLong(id);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readLong();
		lat = in.readUTF();
		lon = in.readUTF();

		outgoing.clear();
		int degree = in.readInt();
		for (int i=0; i<degree; i++)
			outgoing.add(in.readLong());
	}


}
