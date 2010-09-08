package rcg;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;


public class App {

	/**
	 * @param args
	 *            command line arguments
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
	    
		
		Configuration conf = new Configuration();
		
		FileSystem.get(conf).delete(new Path("nodes"));
		FileSystem.get(conf).delete(new Path("output"));
		FileSystem.get(conf).delete(new Path("intermediate"));
		FileSystem.get(conf).delete(new Path("nodes-contracted"));
		
	    ToolRunner.run(conf, new ImportOsm(), new String[] {"input", "nodes"});
	    ToolRunner.run(conf, new Contract(), new String[] {"nodes", "nodes-contracted"});
	    ToolRunner.run(conf, new ExportKml(), new String[] {"nodes-contracted", "output"});
	    
//	    ToolRunner.run(conf, new ExportKml(), new String[] {"nodes", "output"});

	    
	    
	}

	
	    
	    
	// TODO DRIVER 
	// import osm   
	// export kml   
	// compute rcg  argv.. 
	    
//		ProgramDriver pgd = new ProgramDriver();
//		try {
//			pgd.addClass("osm", ImportOsm.class, "import openstreetmap data");
//			pgd.driver(args);
//		} catch (Throwable e) {
//			e.printStackTrace();
//		}
	
}
