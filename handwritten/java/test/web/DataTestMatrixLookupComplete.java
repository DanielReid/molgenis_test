package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixLookupComplete {
	static DataTestMatrix DataTestMatrix = new DataTestMatrix();

	@Test
	public void init() throws Exception {
		DataTestMatrix.dbDriver = "oracle.jdbc.driver.OracleDriver";
		DataTestMatrix.dbUrl = "jdbc:oracle:thin:@//localhost:2000/llptest";
		DataTestMatrix.dbUsername = "MOLGENIS1";
		DataTestMatrix.dbPassword = "talpa010t";

		DataTestMatrix.file = "/pheno_complete.txt";

		DataTestMatrix.testTablePrefix = "TEST_";
		DataTestMatrix.testOwner = "MOLGENIS1";
		DataTestMatrix.sourceTablePrefix = "";
		DataTestMatrix.sourceOwner = "MOLGENIS1";
		DataTestMatrix.matrixSeperator = "\t";
		DataTestMatrix.matrixColumnSeperator = "__";
		DataTestMatrix.matrixStringDateFormat = "yyyy-mm-dd";

		DataTestMatrix.init();
		DataTestMatrix.getMatrixColumnsIndex();
		if (DataTestMatrix.getPA_ID()) {
			Assert.assertFalse(true);
		}
		if (DataTestMatrix.getPublishTablesColumns()) {
			Assert.assertFalse(true);
		}
		DataTestMatrix.makeGlobalTable();
		if (DataTestMatrix.fillGlobalTable()) {
			//Assert.assertFalse(true);
		}
		DataTestMatrix.makeTables();
		DataTestMatrix.fillTables();
	}

	@Test(dependsOnMethods = { "init" })
	public void testCompareTables() throws Exception {
		if (DataTestMatrix.compareTables())
			Assert.assertFalse(true);
	}

}
