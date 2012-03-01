package test.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Locale;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestPivotViews {

	String databaseOracle = "llpacc";
	String dbUsernameOracle = "molgenis3";
	String matrixStringDateFormat = "yyyy-mm-dd";
	boolean testBezoek = true;
	boolean testLabdata = true;

	String sqlSetDateFormat = "alter session set nls_date_format='"
			+ matrixStringDateFormat + "'";
	String dbDriverOracle = "oracle.jdbc.driver.OracleDriver";
	String dbUrlOracle = "jdbc:oracle:thin:@//localhost:2000/" + databaseOracle;
	Statement stmt1;
	ResultSet rset1;
	Statement stmt2;
	ResultSet rset2;
	Statement stmt3;
	ResultSet rset3;

	@Test
	public void testInit() throws Exception {
		init();
	}
	
	@Test(dependsOnMethods = { "testInit" })
	public void testCompare() throws Exception {
		if (compare()) {
			Assert.assertFalse(true);
		}
	}

	public void init() throws IOException {
		Locale.setDefault(Locale.US);
		System.out
				.println("To use outputed SQL from this script in any SQL client execute next line first (once per session) to set the date format right: ");
		System.out.println(sqlSetDateFormat);
		System.out.println();
		System.out.println("Oracle Database: " + databaseOracle);
		System.out.println("Oracle Username: " + dbUsernameOracle);
		System.out.print("Enter Oracle password: ");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String dbPasswordOracle = null;
		dbPasswordOracle = br.readLine();
		try {
			Class.forName(dbDriverOracle);
			Connection conn = DriverManager.getConnection(dbUrlOracle,
					dbUsernameOracle, dbPasswordOracle);
			stmt1 = conn.createStatement();
			stmt1.executeQuery(sqlSetDateFormat);
			stmt2 = conn.createStatement();
			stmt2.executeQuery(sqlSetDateFormat);
			stmt3 = conn.createStatement();
			stmt3.executeQuery(sqlSetDateFormat);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public boolean compare() throws Exception {
		boolean fail = false;
		int totalCount = 0;
		int totalCounter = 0;
		int stepCounter = 0;
		if (testBezoek && tableExists("bezoek") && tableExists("vw_bezoek_pivot"))
			totalCount += tableCount("bezoek");
		if (testLabdata && tableExists("labdata") && tableExists("vw_labdata_pivot"))
			totalCount += tableCount("labdata");

		if (testBezoek && tableExists("bezoek") && tableExists("vw_bezoek_pivot")) {
			System.out.println("Lookup bezoek in vw_bezoek_pivot");
			rset1 = stmt1
					.executeQuery("select pa_id, volgnr, to_char(datum) datum from bezoek");
			while (rset1.next()) {
				if (stepCounter == 0 || stepCounter >= 500) {
					stepCounter = 0;
					System.out.println("[" + totalCounter + "/" + totalCount
							+ "]");
				}
				totalCounter++;
				stepCounter++;
				String sql = "select count(*) from vw_bezoek_pivot where pa_id = '"
						+ rset1.getString("pa_id")
						+ "' and datum_bz"
						+ rset1.getString("volgnr")
						+ " like '"
						+ rset1.getString("datum") + "'";
				rset2 = stmt2.executeQuery(sql);
				rset2.next();
				if (rset2.getInt(1) == 0) {
					fail = true;
					System.out.println("FAILED. Not found in vw_bezoek_pivot: "
							+ sql);
				}
				if (rset2.getInt(1) > 1) {
					fail = true;
					System.out.println("FAILED. Multiple lines: " + sql);
				}
				rset2.close();
			}
			rset1.close();
		}

		if (testLabdata && tableExists("labdata") && tableExists("vw_labdata_pivot")) {
			System.out.println("Lookup labdata in vw_labdata_pivot");
			LinkedHashMap<String, String> nrConv = new LinkedHashMap<String, String>();
			nrConv.put("763", "BL_LE");
			nrConv.put("764", "BL_ER");
			nrConv.put("765", "BL_HB");
			nrConv.put("766", "BL_HT");
			nrConv.put("771", "BL_TR");
			nrConv.put("775", "BL_GR%");
			nrConv.put("776", "BL_LY%");
			nrConv.put("777", "BL_MO%");
			nrConv.put("778", "BL_EO%");
			nrConv.put("779", "BL_BA%");
			nrConv.put("780", "BL_GR#");
			nrConv.put("781", "BL_LY#");
			nrConv.put("782", "BL_MO#");
			nrConv.put("783", "BL_EO#");
			nrConv.put("784", "BL_BA#");
			nrConv.put("787", "BL_SEG");
			nrConv.put("788", "BL_LYM");
			nrConv.put("789", "BL_MON");
			nrConv.put("790", "BL_EOS");
			nrConv.put("791", "BL_BAS");
			nrConv.put("793", "BL_MTA");
			nrConv.put("798", "BL_ATY");
			nrConv.put("1388", "UP_ALBK");
			nrConv.put("1879", "BL_ALPA");
			nrConv.put("1880", "BL_ALPB");
			nrConv.put("1904", "BL_HBAC");
			nrConv.put("10109", "BL_SCRP");
			nrConv.put("10436", "BL_GLU");
			nrConv.put("10437", "BL_NA");
			nrConv.put("10438", "BL_K");
			nrConv.put("10440", "BL_UR");
			nrConv.put("10441", "BL_KR");
			nrConv.put("10442", "BL_UZ");
			nrConv.put("10443", "BL_AST");
			nrConv.put("10444", "BL_ALT");
			nrConv.put("10445", "BL_GGT");
			nrConv.put("10453", "BL_ALB");
			nrConv.put("10454", "BL_CA");
			nrConv.put("10455", "BL_FOS");
			nrConv.put("10457", "BL_TGL");
			nrConv.put("10458", "BL_CHO");
			nrConv.put("10459", "BL_HDC");
			nrConv.put("10460", "BL_LDC");
			nrConv.put("10474", "BL_SGLU");
			nrConv.put("10487", "UP_KR");
			nrConv.put("10645", "BL_AF");
			nrConv.put("11055", "UP_BABU");
			nrConv.put("11165", "BL_EGFR");
			nrConv.put("11993", "BL_HALB");
			nrConv.put("11994", "BL_HAL1");
			nrConv.put("11995", "BL_LCRP");
			nrConv.put("10471", "BL_TSHD");
			nrConv.put("10743", "BL_PFT3");
			nrConv.put("10744", "BL_PFT4");
			nrConv.put("12643", "BL_HB1C");
			nrConv.put("767", "BL_MCV");
			nrConv.put("768", "BL_MCH");
			nrConv.put("769", "BL_MCC");
			nrConv.put("770", "BL_RDW");
			nrConv.put("794", "BL_MYE");
			rset1 = stmt1
					.executeQuery("select pa_id, to_char(observ_dt) observ_dt, elementnr, uitslag from labdata");
			while (rset1.next()) {
				if (stepCounter == 0 || stepCounter >= 500) {
					stepCounter = 0;
					System.out.println("[" + totalCounter + "/" + totalCount
							+ "]");
				}
				totalCounter++;
				stepCounter++;
				String sql = "select count(*) from vw_labdata_pivot where pa_id = "
						+ rset1.getString("pa_id")
						+ " and observ_dt like '"
						+ rset1.getString("observ_dt")
						+ "' and "
						+ nrConv.get(rset1.getString("elementnr"))
						+ " = "
						+ rset1.getString("uitslag");
				rset2 = stmt2.executeQuery(sql);
				rset2.next();
				if (rset2.getInt(1) != 1) {
					String sql1 = "select count(*) from labdata where pa_id = "
							+ rset1.getString("pa_id") + " and observ_dt like '"
							+ rset1.getString("observ_dt") + "' and elementnr = "
							+ rset1.getString("elementnr") + " and uitslag = "
							+ rset1.getString("uitslag");
					if (rset2.getInt(1) == 0) {
						fail = true;
						System.out
								.println("FAILED. Not found in vw_labdata_pivot: ");
						System.out.println(sql);
						System.out.println(sql1);
						System.out.println();
					}
					if (rset2.getInt(1) > 1) {
						rset3 = stmt3.executeQuery(sql1);
						rset3.next();
						if (rset2.getInt(1) != rset3.getInt(1)) {
							fail = true;
							System.out.println("FAILED DIFFERENT ROWCOUNT: ");
							System.out.println(sql);
							System.out.println(sql1);
							System.out.println();
						}
					}
				}
				rset2.close();
			}
			rset1.close();
		}

		System.out.println("[" + totalCounter + "/" + totalCount + "]");
		return fail;
	}

	// Helper function
	public boolean tableExists(String table) throws Exception {
		rset1 = stmt1
				.executeQuery("select count(*) from all_tab_columns where table_name = '"
						+ table.toUpperCase() + "'");
		rset1.next();
		if (rset1.getInt(1) == 0) {
			rset1.close();
			return false;
		}
		rset1.close();
		return true;
	}

	// Helper function
	public int tableCount(String table) throws Exception {
		rset1 = stmt1.executeQuery("select count(*) from " + table);
		rset1.next();
		int tableCount = rset1.getInt(1);
		rset1.close();
		return tableCount;
	}
}
