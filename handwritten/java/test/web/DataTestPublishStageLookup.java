package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestPublishStageLookup {
	static DataTestPublishStage DataTestPublishStage = new DataTestPublishStage();

	@Test
	public void init() throws Exception {

		DataTestPublishStage.counterFailLimit = 999999999;

		DataTestPublishStage.dbDriver1 = "oracle.jdbc.driver.OracleDriver";
		String database = "llptest";
		DataTestPublishStage.dbUrl1 = "jdbc:oracle:thin:@//localhost:2000/"
				+ database;
		DataTestPublishStage.dbUsername1 = "molgenis";
		DataTestPublishStage.dbPassword1 = "molTagtGen24Ora";

		String server2 = "W3ZKHAS323";
		String database2 = "LLCDR_Stage";
		DataTestPublishStage.dbDriver2 = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		DataTestPublishStage.dbUrl2 = "jdbc:sqlserver://" + server2
				+ ";databaseName=" + database2 + ";integratedSecurity=true";

		DataTestPublishStage.excludedTables = new String[] { "UVDEMOG",
				"UVHEALTH", "ONDERZOEKPATIENT", "BLOEDDRUKAVG", "BEZOEK1",
				"ECGLEADS" };
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
		if (DataTestPublishStage.compareData())
			Assert.assertFalse(true);
	}

}
