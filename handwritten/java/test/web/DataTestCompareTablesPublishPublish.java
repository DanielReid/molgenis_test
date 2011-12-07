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
		DataTestCompareTables.dbUsernameOracle = "molgenis";
		DataTestCompareTables.dbPasswordOracle = "molTagtGen24Ora";
		DataTestCompareTables.sqlQueryOracle = "select s.synonym_name, atc.column_name  from all_tab_columns atc left join all_synonyms s on (atc.owner = s.table_owner and atc.table_name = s.table_name) where s.owner = 'MOLGENIS'";

		String serverMSSQL = "W3ZKHAS323";
		String databaseMSSQL = "LLCDR_Publ";
		DataTestCompareTables.dbDriverMSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		DataTestCompareTables.dbUrlMSSQL = "jdbc:sqlserver://" + serverMSSQL
				+ ";databaseName=" + databaseMSSQL + ";integratedSecurity=true";
		DataTestCompareTables.sqlQueryMSSQL = "select TABLE_NAME, COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS";

		DataTestCompareTables.excludedTables = new String[] { "PUBL_DICT",
				"DICT_REF", "SETTINGS", "PUBL_DICT_REFS", "DICT",
				"VW_UVSOCIAL", "VW_BLOEDDRUKAVG", "VW_UVRAND36",
				"VW_MEDICATIE", "VW_UVFEMALE", "VW_UVPANAS", "VW_ECG",
				"VW_ECGLEADS", "VW_SPIROMETRIE", "VW_PATIENT", "VW_ONDERZOEK",
				"VW_UVSTRESS", "VW_LABDATA", "VW_MINIV2", "VW_MINIV3",
				"VW_MMSE", "VW_BEZOEK", "VW_UVDEMOG", "VW_UVHEALTH",
				"VW_BEZOEK1", "VW_ONDERZOEKPATIENT", "VW_MINI", "VW_DICT",
				"VW_UVWORK", "VW_UVNEOP1", "VW_UVSCL90", "VW_BEP_OMSCHR",
				"LABDATA" };
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
