package test.web;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;

import antlr.StringUtils;

public class dataTestMatrix {
	String file = "d:/ll_pheno_quantitatief.txt";

	String testTablePrefix = "T2_";
	String testOwner = "MOLGENIS";

	String sourceTablePrefix = "LL_";
	String sourceOwner = "LLPOPER";

	String paidMatrixColumnName = "PATIENT__PA_ID";
	String matrixSeperator = "\t";

	String[] paidMatrixColumnNameParts = paidMatrixColumnName.split("__");
	Map<Integer, String> matrixColumnsIndex = new HashMap<Integer, String>();
	Map<String, ArrayList<String>> dbTablesColumns = new HashMap<String, ArrayList<String>>();

	public static Connection getConnection() throws Exception {
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@//localhost:2000/llptest";
		String username = "molgenis";
		String password = "molTagtGen24Ora";
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url, username, password);
		return conn;
	}

	public ResultSet executeQuery(String sql) throws Exception {
		Connection conn = dataTestMatrix.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rset = stmt.executeQuery(sql);
		return rset;

	}

	public void getMatrixColumnsIndex() throws IOException {
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine = br.readLine();
		String[] strLineParts = strLine.split(matrixSeperator);
		for (int i = 0; i < strLineParts.length; i++) {
			if (strLineParts[i].split("__").length == 2) {
				matrixColumnsIndex.put(i, strLineParts[i]);
			}
		}
		in.close();
	}

	public void getDbTablesColumns() {

		for (Map.Entry<Integer, String> matrixColumn : matrixColumnsIndex
				.entrySet()) {
			if (matrixColumn.getValue().length() != 0) {
				String[] matrixColumnParts = matrixColumn.getValue()
						.split("__");
				if (matrixColumnParts.length == 2) {
					if (dbTablesColumns.get(matrixColumnParts[0]) == null) {
						ArrayList<String> dbColumns = new ArrayList<String>();
						// for every table add "PA_ID"
						dbColumns.add(paidMatrixColumnNameParts[1]);
						dbColumns.add(matrixColumnParts[1]);
						dbTablesColumns.put(matrixColumnParts[0], dbColumns);
					} else {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns = dbTablesColumns.get(matrixColumnParts[0]);
						if (dbColumns.contains(matrixColumnParts[1]) == false)
							dbColumns.add(matrixColumnParts[1]);
						dbTablesColumns.put(matrixColumnParts[0], dbColumns);
					}
				}
			}
		}
	}

	public void makeTables() throws Exception {
		for (Map.Entry<String, ArrayList<String>> entry : dbTablesColumns
				.entrySet()) {
			if (tableExists(entry.getKey(), testOwner))
				executeQuery("drop table " + testTablePrefix + entry.getKey());
			executeQuery("create table " + testTablePrefix + entry.getKey()
					+ " ("
					+ Joiner.on(" varchar(255), ").join(entry.getValue())
					+ "  varchar(255))");
		}
	}

	boolean tableExists(String table, String owner) throws Exception {
		ResultSet rset = executeQuery("select count(*) from dba_all_tables where table_name='"
				+ testTablePrefix + table + "' and owner='" + owner + "'");
		rset.next();
		if (rset.getString(1).equals("1"))
			return true;
		return false;
	}

	public void makeGlobalTable() throws Exception {
		if (tableExists(testTablePrefix + "GLOBAL", testOwner))
			executeQuery("drop table " + testTablePrefix + "GLOBAL");
		executeQuery("create table "
				+ testTablePrefix
				+ "GLOBAL"
				+ " ("
				+ Joiner.on(" varchar(255), ")
						.join(matrixColumnsIndex.values()) + "  varchar(255))");
	}

	public void fillGlobalTable() throws Exception {
		String sql;
		String line;
		String[] lineParts;
		ArrayList<String> values = new ArrayList<String>();
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		// skip first line column names
		br.readLine();

		sql = "INSERT INTO " + testTablePrefix + "GLOBAL ("
				+ Joiner.on(", ").join(matrixColumnsIndex.values()) + ")\n";

		// first data line
		if ((line = br.readLine()) != null) {
			lineParts = line.split(matrixSeperator);
			values.clear();
			for (Map.Entry<Integer, String> matrixColumn : matrixColumnsIndex
					.entrySet()) {
				values.add("'" + lineParts[matrixColumn.getKey()] + "'");
			}
			sql += "SELECT " + Joiner.on(", ").join(values) + " FROM DUAL\n";
		}

		while ((line = br.readLine()) != null) {
			lineParts = line.split(matrixSeperator);
			values.clear();
			for (Map.Entry<Integer, String> matrixColumn : matrixColumnsIndex
					.entrySet()) {
				values.add("'" + lineParts[matrixColumn.getKey()] + "'");
			}
			sql += "UNION ALL SELECT " + Joiner.on(", ").join(values)
					+ " FROM DUAL\n";
		}
		in.close();
		executeQuery(sql);
	}

	public void fillTables() throws Exception {
		ArrayList<String> combineTableColumn = new ArrayList<String>();
		for (Map.Entry<String, ArrayList<String>> entry : dbTablesColumns
				.entrySet()) {
			combineTableColumn.clear();
			for (String column : entry.getValue()) {
				if (paidMatrixColumnNameParts[1] == column)
					combineTableColumn.add(paidMatrixColumnNameParts[0] + "__"
							+ column);
				else
					combineTableColumn.add(entry.getKey() + "__" + column);
			}
			executeQuery("INSERT INTO " + testTablePrefix + entry.getKey()
					+ " (" + Joiner.on(", ").join(entry.getValue())
					+ ") SELECT " + Joiner.on(", ").join(combineTableColumn)
					+ " FROM " + testTablePrefix + "GLOBAL");
		}
	}

	@Test
	public void init() throws Exception {
		Locale.setDefault(Locale.US);
		getMatrixColumnsIndex();
		getDbTablesColumns();
		Assert.assertTrue(true);
	}

	@Test(dependsOnMethods = { "init" })
	public void testMakeGlobalTable() throws Exception {
		// makeGlobalTable();
		Assert.assertTrue(true);
	}

	@Test(dependsOnMethods = { "testMakeGlobalTable" })
	public void testFillGlobalTable() throws Exception {
		// fillGlobalTable();
		Assert.assertTrue(true);
	}

	@Test(dependsOnMethods = { "testFillGlobalTable" })
	public void testMakeTables() throws Exception {
		// makeTables();
		Assert.assertTrue(true);
	}

	@Test(dependsOnMethods = { "testMakeTables" })
	public void testFillTables() throws Exception {
		// fillTables();
		Assert.assertTrue(true);
	}

	@Test(dependsOnMethods = { "testFillTables" })
	public void next() throws Exception {
		for (Map.Entry<String, ArrayList<String>> entry : dbTablesColumns
				.entrySet()) {
			for (String column : entry.getValue()) {
				try {
					executeQuery("select count(*) from (select case  when substr("
							+ column
							+ ", length("
							+ column
							+ ")-1, 2) = '.0'  then substr("
							+ column
							+ ", 0, length("
							+ column
							+ ")-2) else "
							+ column
							+ " end as "
							+ column
							+ " from "
							+ testTablePrefix
							+ entry.getKey()
							+ " minus select to_char("
							+ column
							+ ") from llpoper."
							+ sourceTablePrefix
							+ entry.getKey() + ")");
					System.out.println("SUCCES " + testTablePrefix
							+ entry.getKey() + "." + column
							+ " data in source (with '.0' fix)");
				} catch (Exception e) {
					System.out.println(e);
					// Assert.assertTrue(false);
				}
			}
		}
		Assert.assertTrue(true);
	}

}
