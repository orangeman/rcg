

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.util.ToolRunner;


public class OsmTool {

	/**
	 * @param args
	 *            command line arguments
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
	    
	    int result = ToolRunner.run(new Configuration(), new Contractor(), args);
	    System.out.println(result);
	    
	    // DRIVER  JobControll
	    // import osm   
	    // export kml   
	    // compute rcg  argv.. 
	    
	    
//		int exitCode = -1;
//		ProgramDriver pgd = new ProgramDriver();
//		try {
//			pgd.addClass("osm", Contractor.class, "checking the number of <page> entries");
//			pgd.driver(args);
//		} catch (Throwable e) {
//			e.printStackTrace();
//		}
//
//		System.exit(exitCode);
	}

}
