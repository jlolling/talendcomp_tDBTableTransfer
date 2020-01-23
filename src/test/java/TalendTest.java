import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TalendTest {
	
	
	protected Map<String, Object> globalMap = new HashMap<String, Object>(); 	
	protected boolean globalResumeTicket = true;
	protected String resumeEntryMethodName = null;
	protected Map<String, Boolean> ok_Hash = new HashMap<String, Boolean>();
	protected Map<String, Long> start_Hash = new HashMap<String, Long>();
	protected String currentComponent = null;
	protected boolean execStat = false;
	
	public static Connection createConnection(String propertyFilePath) throws Exception {
		Properties props = new Properties();
		props.load(new BufferedInputStream(new FileInputStream(new File(propertyFilePath))));
		Class.forName(props.getProperty("DRIVER_CLASS"));
		return DriverManager.getConnection(props.getProperty("URL"), props.getProperty("USER"), props.getProperty("PW"));
	}

}