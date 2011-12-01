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
		// String server2 = "09-000-4718\\SQLEXPRESS";
		String database2 = "LLCDR_Publ";
		DataTestCompareTables.dbDriverMSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		DataTestCompareTables.dbUrlMSSQL = "jdbc:sqlserver://" + server2
				+ ";databaseName=" + database2 + ";integratedSecurity=true";

		/*
		 * DataTestCompareTables.excludedTables = new String[] { "VW_LABDATA",
		 * "PATIENT", "UVPANAS", "VW_DICT_REFS", "VW_BLOEDDRUKAVG", "BEZOEK",
		 * "VW_UVPANAS", "VW_BEZOEK", "VW_PATIENT", "VW_BEZOEK1",
		 * "BLOEDDRUKAVG", "VW_DICT", "VW_DICT_VALUESETS", "BEZOEK1",
		 * "BEP_OMSCHR", "VW_BEP_OMSCHR" };
		 */

		DataTestCompareTables.excludedTables = new String[] {};
		DataTestCompareTables.excludedColumns = new String[] {};

		DataTestCompareTables.init();
		DataTestCompareTables.getPublishUmcgTablesColumns();
		DataTestCompareTables.getPublishCitTablesColumns();
	}

	@Test(dependsOnMethods = { "init" })
	public void testCompareTableColumns() throws Exception {
		if (DataTestCompareTables.compareTableColumns()) {
			Assert.assertFalse(true);
		}
	}

	@Test(dependsOnMethods = { "testCompareTableColumns" })
	public void testRowCountUmcgVsCit() throws Exception {
		if (DataTestCompareTables.rowCountUmcgVersusCit()) {
			Assert.assertFalse(true);
		}
	}

	@Test(dependsOnMethods = { "testRowCountUmcgVsCit" })
	public void testLookupDataUmcgInCit() throws Exception {
		if (DataTestCompareTables.lookupDataUmcgInCit()) {
			Assert.assertFalse(true);
		}
	}

	@Test(dependsOnMethods = { "testLookupDataUmcgInCit" })
	public void testLookupDataCitInUmcg() throws Exception {
		if (DataTestCompareTables.lookupDataCitInUmcg()) {
			Assert.assertFalse(true);
		}
	}

}
