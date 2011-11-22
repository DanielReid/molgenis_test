package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixLookupTwoFilters {
	static DataTestMatrix DataTestMatrix = new DataTestMatrix();

	@Test
	public void init() throws Exception {
		DataTestMatrix.dbDriver = "oracle.jdbc.driver.OracleDriver";
		DataTestMatrix.dbUrl = "jdbc:oracle:thin:@//localhost:2000/llptest";
		DataTestMatrix.dbUsername = "molgenis";
		DataTestMatrix.dbPassword = "molTagtGen24Ora";

		DataTestMatrix.file = "/pheno_two_filters.txt";

		DataTestMatrix.testTablePrefix = "T2_";
		DataTestMatrix.testOwner = "MOLGENIS";
		DataTestMatrix.sourceTablePrefix = "";
		DataTestMatrix.sourceOwner = "LLPOPER";
		DataTestMatrix.matrixSeperator = "\t";

		DataTestMatrix.filter1Table = "UVPANAS";
		DataTestMatrix.filter1Column = "PANAS1";
		DataTestMatrix.filter1Operator = "=";
		DataTestMatrix.filter1Value = "4";

		DataTestMatrix.filter2Table = "UVPANAS";
		DataTestMatrix.filter2Column = "PANAS2";
		DataTestMatrix.filter2Operator = "=";
		DataTestMatrix.filter2Value = "3";

		DataTestMatrix.init();
		DataTestMatrix.getMatrixColumnsIndex();
		if (DataTestMatrix.getPA_ID())
			Assert.assertFalse(true);
		if (DataTestMatrix.getPublishTablesColumns())
			Assert.assertFalse(true);
		DataTestMatrix.makeGlobalTable();
		DataTestMatrix.fillGlobalTable();
		DataTestMatrix.makeTables();
		DataTestMatrix.fillTables();
	}

	@Test(dependsOnMethods = { "init" })
	public void testCompareTables() throws Exception {
		if (DataTestMatrix.compareTables())
			Assert.assertFalse(true);
	}

}
