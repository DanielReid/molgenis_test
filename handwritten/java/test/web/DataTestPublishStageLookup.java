package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestPublishStageLookup {
	static DataTestPublishStage DataTestPublishStage = new DataTestPublishStage();

	@Test
	public void init() throws Exception {

		DataTestPublishStage.studie = "Test001";

		DataTestPublishStage.dbDriver1 = "oracle.jdbc.driver.OracleDriver";
		DataTestPublishStage.dbUrl1 = "jdbc:oracle:thin:@//localhost:2000/llptest";
		DataTestPublishStage.dbUsername1 = "molgenis";
		DataTestPublishStage.dbPassword1 = "molTagtGen24Ora";

		DataTestPublishStage.dbDriver2 = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		DataTestPublishStage.dbUrl2 = "jdbc:sqlserver://W3ZKHAS323;databaseName=LLCDR_Stage;integratedSecurity=true";
		DataTestPublishStage.dbUsername2 = "sa";
		DataTestPublishStage.dbPassword2 = "123456";

		DataTestPublishStage.publishOwnerPrefix = "LLPOPER.";
		DataTestPublishStage.tableNameReplaceFrom = "LL_";
		DataTestPublishStage.tableNameReplaceTo = "";

		// DEFAULTS:
		/*
		 * DataTestPublishStage.excludedTables = new String[] {
		 * "LL_DERIVED_VALUES" }; DataTestPublishStage.excludedColumns = new
		 * String[] { "STID", "PA_ID" };
		 */

		DataTestPublishStage.excludedTables = new String[] {
				"LL_DERIVED_VALUES", "LL_UVFAM" };
		DataTestPublishStage.excludedColumns = new String[] { "STID", "PA_ID",
				"GEBJAAR", "GEMEENTENAAM", "ID", "OVLJAAR", "PARTICIPST",
				"UITSLOPM", "DATUM", "RACE", "RACE_TXT", "DATUM", "MEETSTAND",
				"HR", "P", "PQ", "P_AXIS", "QRS", "QRS_AXIS", "QT", "QTC",
				"T_AXIS" };

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
