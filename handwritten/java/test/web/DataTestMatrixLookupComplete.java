package test.web;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import au.com.bytecode.opencsv.CSVReader;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixLookupComplete {

	// Input CSV file
	String file = "H:/Data/Exports/20120201 Export Matrix Viewer/Export_All_2012-02-01_09_44_47.csv";

	// Oracle parameters
	String dbDriverOracle = "oracle.jdbc.driver.OracleDriver";
	String databaseOracle = "llptest";
	String dbUrlOracle = "jdbc:oracle:thin:@//localhost:2000/" + databaseOracle;
	String dbUsernameOracle = "molgenis";

	// Prefix and owner details
	String testTablePrefix = "TEST_";
	String sourceTablePrefix = "";
	String matrixColumnSeperator = "__";
	String matrixStringDateFormat = "yyyy-mm-dd";

	String testOwner = dbUsernameOracle;
	String sourceOwner = dbUsernameOracle;
	Statement stmt;
	ResultSet rset;
	String[] paidMatrixColumnNameParts;
	Map<String, Integer> matrixColumnsIndex = new HashMap<String, Integer>();
	Map<String, ArrayList<String>> publishTablesColumns = new HashMap<String, ArrayList<String>>();

	@Test
	public void testInit() throws Exception {
		init();
		getMatrixColumnsIndex();
		if (getPA_ID()) {
			Assert.assertFalse(true);
		}
		if (getPublishTablesColumns()) {
			Assert.assertFalse(true);
		}
		makeGlobalTable();
		if (fillGlobalTable()) {
			Assert.assertFalse(true);
		}
		makeTables();
		fillTables();
	}

	@Test(dependsOnMethods = { "testInit" })
	public void testCompareTables() throws Exception {
		if (compareTables())
			Assert.assertFalse(true);
	}

	public void init() throws IOException {
		Locale.setDefault(Locale.US);
		System.out.print("Enter Oracle password for database '"
				+ databaseOracle + "' and user '" + dbUsernameOracle + "':");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String dbPasswordOracle = null;
		dbPasswordOracle = br.readLine();		
		try {	
			Class.forName(dbDriverOracle);
			Connection conn = DriverManager.getConnection(dbUrlOracle, dbUsernameOracle,
					dbPasswordOracle);
			stmt = conn.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void getMatrixColumnsIndex() throws Exception {
		System.out.println("Getting Matrix column index...");
		CSVReader reader = new CSVReader(new InputStreamReader(
				new FileInputStream(file), "UTF-8"));
		String[] headerLine = reader.readNext();

//		TODO
//		for (int i = 0; i < headerLine.length; i++) {
//			System.out.println(headerLine[i]);
//			String sql = "select table_name, column_name from ALL_TAB_COLUMNS where owner ='"
//					+ testOwner + "' and column_name='" + headerLine[i] + "'";
//			rset = stmt.executeQuery("select count(*) from (" + sql + ")");
//			rset.next();
//			System.out.println(rset.getString(1));
//			System.out.println(rset.getString(2));
//		}

		for (int i = 0; i < headerLine.length; i++) {
			if (headerLine[i].length() != 0) {
				String[] matrixColumnParts = headerLine[i]
						.split(matrixColumnSeperator);
				if (matrixColumnParts.length == 2) {
					if (tableColumnExists(sourceOwner, sourceTablePrefix
							+ matrixColumnParts[0], matrixColumnParts[1])) {
						matrixColumnsIndex.put(headerLine[i], i);
					} else {
						System.out
								.println("WARNING: '"
										+ sourceTablePrefix
										+ matrixColumnParts[0]
										+ "."
										+ matrixColumnParts[1]
										+ "' (based on matrix column) not in source. IGNORING!");
					}
				} else {
					System.out
							.println("WARNING: '"
									+ headerLine[i]
									+ "' not conform metadata format. (can not extract the source table and column name). IGNORING!");
				}
			}
		}
	}

	public Boolean getPA_ID() {
		for (String column : matrixColumnsIndex.keySet()) {
			if (column.contains("PA_ID")) {
				paidMatrixColumnNameParts = column.split(matrixColumnSeperator);
				if (paidMatrixColumnNameParts.length == 2) {
					return false;
				}
			}
		}
		System.out.println("FAIL: No PA_ID found");
		return true;
	}

	public boolean getPublishTablesColumns() throws Exception {
		System.out.println("Generating Publish table column structure...");
		boolean fail = false;
		for (Map.Entry<String, Integer> matrixColumn : matrixColumnsIndex
				.entrySet()) {
			String[] matrixColumnParts = matrixColumn.getKey().split(
					matrixColumnSeperator);
			if (publishTablesColumns.get(matrixColumnParts[0]) == null) {
				ArrayList<String> dbColumns = new ArrayList<String>();
				dbColumns.add(matrixColumnParts[1]);
				publishTablesColumns.put(matrixColumnParts[0], dbColumns);
			} else {
				ArrayList<String> dbColumns = new ArrayList<String>();
				dbColumns = publishTablesColumns.get(matrixColumnParts[0]);
				if (dbColumns.contains(matrixColumnParts[1]) == false)
					dbColumns.add(matrixColumnParts[1]);
				publishTablesColumns.put(matrixColumnParts[0], dbColumns);
			}
		}
		return fail;
	}

	public void makeGlobalTable() throws Exception {
		System.out.println("Making global table...");
		if (tableExists(testOwner, testTablePrefix + "GLOBAL"))
			stmt.execute("drop table " + testTablePrefix + "GLOBAL");
		String sql = "create table " + testTablePrefix + "GLOBAL (";
		for (Integer key : matrixColumnsIndex.values())
			sql += "c" + key + " varchar(255), ";
		sql = sql.substring(0, sql.length() - 2) + ")";
		stmt.execute(sql);
	}

	public boolean fillGlobalTable() throws Exception {
		boolean fail = false;
		System.out.println("Filling global table...");
		String insertIntoSql = "insert into " + testTablePrefix + "GLOBAL (";
		for (Integer key : matrixColumnsIndex.values())
			insertIntoSql += "c" + key + ", ";
		insertIntoSql = insertIntoSql.substring(0, insertIntoSql.length() - 2)
				+ ") ";
		CSVReader reader = new CSVReader(new InputStreamReader(
				new FileInputStream(file), "UTF-8"));
		String[] line;
		// Skip header.
		reader.readNext();
		Integer totalCounter = 1;
		Integer counter = 1;
		while ((line = reader.readNext()) != null) {
			totalCounter++;
			counter++;
			if (counter >= 500) {
				counter = 0;
				System.out.println("Rowcount: " + totalCounter);
			}
			Boolean exception = false;
			String selectSql = "select ";

			for (Integer key : matrixColumnsIndex.values()) {
				try {
					selectSql += "'" + line[key].replace("'", "''") + "', ";
				} catch (Exception e) {
					exception = true;
					fail = true;
				}
			}
			if (exception) {
				String linestr = "";
				for (int i = 0; i < line.length; i++) {
					linestr += line[i] + ",";
				}
				linestr = selectSql.substring(0, selectSql.length() - 1);
				System.out.println("FAIL: Wrong line length. Line:" + linestr
						+ "END");
			} else {
				selectSql = selectSql.substring(0, selectSql.length() - 2)
						+ " from dual";
				// System.out.println(insertIntoSql + selectSql);
				stmt.executeQuery(insertIntoSql + selectSql);
			}
		}
		return fail;
	}

	public void makeTables() throws Exception {
		System.out.println("Make tables...");
		for (Map.Entry<String, ArrayList<String>> entry : publishTablesColumns
				.entrySet()) {
			if (tableExists(testOwner, testTablePrefix + entry.getKey()))
				stmt.executeQuery("drop table " + testTablePrefix
						+ entry.getKey());
			String sql = "create table " + testTablePrefix + entry.getKey()
					+ " (";
			for (String value : entry.getValue())
				sql += value + " varchar(255), ";
			sql = sql.substring(0, sql.length() - 2) + ")";
			stmt.executeQuery(sql);
		}
	}

	public void fillTables() throws Exception {
		System.out.println("Filling tables...");
		for (Map.Entry<String, ArrayList<String>> entry : publishTablesColumns
				.entrySet()) {
			String table = entry.getKey();
			String sql = "insert into " + testTablePrefix + entry.getKey()
					+ " (";
			for (String column : entry.getValue())
				sql += column + ", ";
			sql = sql.substring(0, sql.length() - 2) + ") ";
			sql += "select ";
			for (String column : entry.getValue())
				sql += "c"
						+ matrixColumnsIndex.get(table + matrixColumnSeperator
								+ column) + ", ";
			sql = sql.substring(0, sql.length() - 2) + " from "
					+ testTablePrefix + "GLOBAL ";
			stmt.executeQuery(sql);
		}
	}

	public boolean compareTables() throws Exception {
		boolean fail = false;
		for (Map.Entry<String, ArrayList<String>> entry : publishTablesColumns
				.entrySet()) {
			String table = entry.getKey();
			System.out.println("Comparing " + table + "...");

			String testTableSql = "select ";
			for (String column : entry.getValue())
				testTableSql += "case when upper(" + column
						+ ") ='NULL' then null else " + column + " end as "
						+ column + ", ";
			testTableSql = testTableSql.substring(0, testTableSql.length() - 2)
					+ " from " + testTablePrefix + table + " ";
			// Filter rows with only NULL values because this can be a permitted
			// result from joining multiple tables.
			testTableSql += "where ";
			for (String column : entry.getValue())
				testTableSql += column + " is not null and ";
			testTableSql = testTableSql.substring(0, testTableSql.length() - 4);

			String baseTableSql = "select ";
			for (String column : entry.getValue()) {
				rset = stmt
						.executeQuery("select data_type from ALL_TAB_COLUMNS where owner = '"
								+ testOwner
								+ "' and table_name='"
								+ table
								+ "' and column_name = '" + column + "'");
				rset.next();
				if (rset.getString(1).equals("DATE"))
					baseTableSql += "to_char(" + column + ", '"
							+ matrixStringDateFormat + "'), ";
				else
					baseTableSql += "to_char(" + column + "), ";
			}
			baseTableSql = baseTableSql.substring(0, baseTableSql.length() - 2)
					+ " from " + table;

			String sql = "select count(*) from (" + testTableSql + " minus "
					+ baseTableSql + ") ";
			rset = stmt.executeQuery(sql);
			rset.next();
			if (Integer.parseInt(rset.getString(1)) == 0)
				System.out.println("SUCCES " + table + " data in source.");
			else {
				fail = true;
				System.out.println("FAILED datacompare for: " + table);
				System.out.println(sql);
			}
		}
		return fail;
	}

	boolean tableExists(String owner, String table) throws Exception {
		rset = stmt
				.executeQuery("select count(*) from all_all_tables where owner='"
						+ owner + "' and table_name='" + table + "'");
		rset.next();
		if (rset.getString(1).equals("1"))
			return true;
		return false;
	}

	boolean tableColumnExists(String owner, String table, String column)
			throws Exception {
		String sql = "select count(*) from all_tab_columns where owner='"
				+ owner + "' and table_name='" + table
				+ "' and column_name = '" + column + "'";
		rset = stmt.executeQuery(sql);
		rset.next();
		if (rset.getString(1).equals("1"))
			return true;
		return false;
	}

}
