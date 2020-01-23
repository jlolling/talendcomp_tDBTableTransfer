import java.sql.Connection;

public class PlayOracleTransfer extends TalendTest {

	private Connection connectionInput = null;
	private Connection connectionOutput = null;
	private static final String configFileIn = "/Data/projects/lufthansa/svn/AMOS_DLH_LCAG_CONFIG/amos_transfer/config/context_AMOS_TRANSFER.properties";
	private static final String configFileOut = "/Data/Talend/testdata/oracle_test_db.properties";

	public static void main(String[] args) {
		try {
			new PlayOracleTransfer().testTransfer();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	public void testTransfer() throws Exception {
		Connection connIn = createConnection(configFileIn);
	}

}
