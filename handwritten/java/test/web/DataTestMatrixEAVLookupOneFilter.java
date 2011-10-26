package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixEAVLookupOneFilter {
	static DataTestMatrixEAV DataTestMatrixEAV = new DataTestMatrixEAV();

	@Test
	public void init() throws Exception {

		DataTestMatrixEAV.investigation = "50";
		DataTestMatrixEAV.file = "/pheno_one_filter.txt";

		DataTestMatrixEAV.testTablePrefix = "T2_";
		DataTestMatrixEAV.sourceTablePrefix = "LL_";
		DataTestMatrixEAV.matrixSeperator = "\t";
		DataTestMatrixEAV.testOwner = "MOLGENIS";

		DataTestMatrixEAV.dbDriver = "oracle.jdbc.driver.OracleDriver";
		DataTestMatrixEAV.dbUrl = "jdbc:oracle:thin:@//localhost:2000/llptest";
		DataTestMatrixEAV.dbUsername = "molgenis";
		DataTestMatrixEAV.dbPassword = "molTagtGen24Ora";

		// inverse filter
		String filter1Table = "BEZOEK1";
		String filter1Column = "MEETSTAND";
		String filter1Operator = "<>";
		String filter1Value = "1";
		
		DataTestMatrixEAV.caseWhenEndAs = "end as";
		DataTestMatrixEAV.caseWhenCondition = "case when (p.name = '"
				+ DataTestMatrixEAV.sourceTablePrefix + filter1Table
				+ "' and oe.name = '" + filter1Column + "' and ov.value "
				+ filter1Operator + " '" + filter1Value + "') then null else ";

		DataTestMatrixEAV.init();

		if (DataTestMatrixEAV.parseMatrixColumns() == false)
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "init" })
	public void testparseMatrixColumns() throws Exception {
		if (DataTestMatrixEAV.parseMatrixColumns() == false)
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "testparseMatrixColumns" })
	public void testFillGlobalTable() throws Exception {
		DataTestMatrixEAV.makeGlobalTable();
		DataTestMatrixEAV.fillGlobalTable();
	}

	@Test(dependsOnMethods = { "testFillGlobalTable" })
	public void testCompareGlobalTableToEAV() throws Exception {
		if (DataTestMatrixEAV.compareGlobalTableToEAV() == false)
			Assert.assertFalse(true);
	}
}
