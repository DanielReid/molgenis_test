package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixLookupOneFilter {
	static DataTestMatrix DataTestMatrix = new DataTestMatrix();

	@Test
	public void init() throws Exception {
		DataTestMatrix.dbDriver = "oracle.jdbc.driver.OracleDriver";
		DataTestMatrix.dbUrl = "jdbc:oracle:thin:@//localhost:2000/llptest";
		DataTestMatrix.dbUsername = "molgenis";
		DataTestMatrix.dbPassword = "molTagtGen24Ora";

		DataTestMatrix.file = "/pheno_one_filter.txt";

		DataTestMatrix.testTablePrefix = "T2_";
		DataTestMatrix.testOwner = "MOLGENIS";
		DataTestMatrix.sourceTablePrefix = "";
		DataTestMatrix.sourceOwner = "LLPOPER";
		DataTestMatrix.matrixSeperator = "\t";

		DataTestMatrix.filter1Table = "UVPANAS";
		DataTestMatrix.filter1Column = "PANAS1";
		DataTestMatrix.filter1Operator = "=";
		DataTestMatrix.filter1Value = "4";

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
