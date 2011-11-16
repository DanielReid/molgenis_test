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
		DataTestMatrix.file = "/pheno_complete.txt";
		DataTestMatrix.testTablePrefix = "T2_";
		DataTestMatrix.testOwner = "MOLGENIS";
		DataTestMatrix.sourceTablePrefix = "LL_";
		DataTestMatrix.sourceOwner = "LLPOPER";
		DataTestMatrix.matrixSeperator = "\t";

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
	public void testCompareTablesTwoFilters() throws Exception {
		if (DataTestMatrix.compareFilteredTables("BEZOEK1", "GEWICHT", "=",
				"50", "PATIENT", "GESLACHT", "=", "2") == false)
			Assert.assertFalse(true);
	}

}