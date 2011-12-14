package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestPublishStageLookup {
	static DataTestPublishStage DataTestPublishStage = new DataTestPublishStage();

	@Test
	public void init() throws Exception {
		DataTestPublishStage.counterFailLimit = 999999999;

		DataTestPublishStage.dbDriverOracle = "oracle.jdbc.driver.OracleDriver";
		String databaseOracle = "llptest";
		DataTestPublishStage.dbUrlOracle = "jdbc:oracle:thin:@//localhost:2000/"
				+ databaseOracle;
		DataTestPublishStage.dbUsernameOracle = "molgenis";
		DataTestPublishStage.dbPasswordOracle = "molTagtGen24Ora";
		DataTestPublishStage.metadataQueryOracle = "select tabnaam, veld from vw_dict group by tabnaam, veld";

		String serverMSSQL = "W3ZKHAS323";
		String databaseMSSQL = "LLCDR_Stage";
		DataTestPublishStage.dbDriverMSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		DataTestPublishStage.dbUrlMSSQL = "jdbc:sqlserver://" + serverMSSQL
				+ ";databaseName=" + databaseMSSQL + ";integratedSecurity=true";

		DataTestPublishStage.excludedTables = new String[] { "UVHEALTH", "ONDERZOEKPATIENT", "BLOEDDRUKAVG", "BEZOEK1", "UVDEMOG", "ECGLEADS" };
		DataTestPublishStage.excludedColumns = new String[] { "PA_ID" };

		DataTestPublishStage.init();
		DataTestPublishStage.getPublishTablesColumns();
		DataTestPublishStage.getTotalRecordCount();
	}

	@Test(dependsOnMethods = { "init" })
	public void testCompareTableColums() throws Exception {
		if (DataTestPublishStage.compareTableColums())
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "testCompareTableColums" })
	public void testCompareData() throws Exception {
		if (DataTestPublishStage.lookupPublishStage())
			Assert.assertFalse(true);
	}

}
