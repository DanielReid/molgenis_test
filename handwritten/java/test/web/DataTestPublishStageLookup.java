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
		DataTestPublishStage.dbUrl2 = "jdbc:sqlserver://ANCO-PC;databaseName=stage";
		DataTestPublishStage.dbUsername2 = "sa";
		DataTestPublishStage.dbPassword2 = "123456";

		DataTestPublishStage.publishOwnerPrefix = "LLPOPER.";
		DataTestPublishStage.stageOwnerPrefix = "";
		
		DataTestPublishStage.excludedTables = new String[] { "LL_DERIVED_VALUES" };
		DataTestPublishStage.excludedColumns = new String[] { "STID", "PA_ID",
				"ID", "DATUM" };

		DataTestPublishStage.init();
		DataTestPublishStage.getPublishTablesColumns();
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
