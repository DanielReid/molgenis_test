package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestCompareTablesPublishPublish {
	static DataTestCompareTables DataTestCompareTables = new DataTestCompareTables();

	@Test
	public void init() throws Exception {
		DataTestCompareTables.dbDriverOracle = "oracle.jdbc.driver.OracleDriver";
		String database1 = "llptest";
		DataTestCompareTables.dbUrlOracle = "jdbc:oracle:thin:@//localhost:2000/"
				+ database1;
		DataTestCompareTables.dbUsernameOracle = "molgenis";
		DataTestCompareTables.dbPasswordOracle = "molTagtGen24Ora";

		String server2 = "W3ZKHAS323";
		String database2 = "LLCDR_Publ";
		DataTestCompareTables.dbDriverMSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		DataTestCompareTables.dbUrlMSSQL = "jdbc:sqlserver://" + server2
				+ ";databaseName=" + database2 + ";integratedSecurity=true";

		DataTestCompareTables.excludedTables = new String[] { "PUBL_DICT",
				"PUBL_DICT_REFS", "SETTINGS", "PATIENT", "VW_DICT", "LABDATA",
				"BLOEDDRUKAVG", "UVPANAS", "VW_DICT_VALUESETS" };
		DataTestCompareTables.excludedColumns = new String[] { "ID" };

		DataTestCompareTables.init();
		DataTestCompareTables.getPublishUmcgTablesColumns();

	}

	@Test(dependsOnMethods = { "init" })
	public void testLookupTablesColumnsUmcgInCit() throws Exception {
		if (DataTestCompareTables.lookupTablesColumnsUmcgInCit())
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "testLookupTablesColumnsUmcgInCit" })
	public void testRowCountUmcgVsCit() throws Exception {
		if (DataTestCompareTables.rowCountUmcgVsCit())
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "testRowCountUmcgVsCit" })
	public void testLookupDataUmcgInCit() throws Exception {
		if (DataTestCompareTables.lookupDataUmcgInCit())
			Assert.assertFalse(true);
	}

}
