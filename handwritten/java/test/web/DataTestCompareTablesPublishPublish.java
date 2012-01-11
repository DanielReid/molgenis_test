package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestCompareTablesPublishPublish {
	static DataTestCompareTables DataTestCompareTables = new DataTestCompareTables();

	@Test
	public void init() throws Exception {
		DataTestCompareTables.counterFailLimit = 999999999;

		DataTestCompareTables.dbDriverOracle = "oracle.jdbc.driver.OracleDriver";
		String databaseOracle = "llptest";
		DataTestCompareTables.dbUrlOracle = "jdbc:oracle:thin:@//localhost:2000/"
				+ databaseOracle;
		DataTestCompareTables.dbUsernameOracle = "molgenis1";
		DataTestCompareTables.dbPasswordOracle = "talpa010t";
		DataTestCompareTables.metadataQueryOracle = "select tabnaam, veld from vw_dict group by tabnaam, veld";
		
		String serverMSSQL = "W3ZKHAS323";
		String databaseMSSQL = "LLCDR_Publ";
		DataTestCompareTables.dbDriverMSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		DataTestCompareTables.dbUrlMSSQL = "jdbc:sqlserver://" + serverMSSQL
				+ ";databaseName=" + databaseMSSQL + ";integratedSecurity=true";

		DataTestCompareTables.excludedTables = new String[] {};
		DataTestCompareTables.excludedColumns = new String[] {};

		DataTestCompareTables.init();
		DataTestCompareTables.getPublishUmcgTablesColumns();
		DataTestCompareTables.getPublishCitTablesColumns();
	}

	@Test(dependsOnMethods = { "init" })
	public void testCompareTableColumns() throws Exception {
		if (DataTestCompareTables.compareTableColumns())
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "testCompareTableColumns" })
	public void testRowCountUmcgVsCit() throws Exception {
		if (DataTestCompareTables.rowCountUmcgVersusCit())
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "testRowCountUmcgVsCit" })
	public void testLookupDataUmcgInCit() throws Exception {
		if (DataTestCompareTables.lookupDataUmcgInCit()) {
			// Assert.assertFalse(true);
		}
	}

	@Test(dependsOnMethods = { "testLookupDataUmcgInCit" })
	public void testLookupDataCitInUmcg() throws Exception {
		if (DataTestCompareTables.lookupDataCitInUmcg())
			Assert.assertFalse(true);
	}

}
