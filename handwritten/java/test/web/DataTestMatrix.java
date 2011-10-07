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

	public void getMatrixColumnsIndex() throws Exception {
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine = br.readLine();
		String[] matrixColumns = strLine.split(matrixSeperator);
		for (int i = 0; i < matrixColumns.length; i++) {
			if (matrixColumns[i].length() != 0)
				matrixColumnsIndex.put(i, matrixColumns[i]);
		}
		in.close();
	}

	public boolean getDbTablesColumns() throws Exception {
		boolean result = true;
		for (Map.Entry<Integer, String> matrixColumn : matrixColumnsIndex
				.entrySet()) {
			String[] matrixColumnParts = matrixColumn.getValue().split("__");
			if (matrixColumnParts.length == 2) {
				if (tableColumnExists(sourceOwner, sourceTablePrefix
						+ matrixColumnParts[0], matrixColumnParts[1])) {
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
				} else {
					System.out.println("WARNING: '" + sourceTablePrefix
							+ matrixColumnParts[0] + "." + matrixColumnParts[1]
							+ "' (based on matrix column) not in source");
					result = false;
				}
			} else {
				System.out
						.println("WARNING: '"
								+ matrixColumn.getValue()
								+ "' not conform format. (can not extract the source table and column name)");
				result = false;
			}
		}
		return result;
	}

	public void makeTables() throws Exception {
		for (Map.Entry<String, ArrayList<String>> entry : dbTablesColumns
				.entrySet()) {
			if (tableExists(testOwner, testTablePrefix + entry.getKey()))
				executeQuery("drop table " + testTablePrefix + entry.getKey());
			executeQuery("create table " + testTablePrefix + entry.getKey()
					+ " ("
					+ Joiner.on(" varchar(255), ").join(entry.getValue())
					+ "  varchar(255))");
		}
	}

	boolean tableExists(String owner, String table) throws Exception {
		ResultSet rset = executeQuery("select count(*) from dba_all_tables where owner='"
				+ owner + "' and table_name='" + table + "'");
		rset.next();
		if (rset.getString(1).equals("1"))
			return true;
		return false;
	}

	boolean tableColumnExists(String owner, String table, String column)
			throws Exception {
		ResultSet rset = executeQuery("select count(*) from dba_tab_columns where owner='"
				+ owner
				+ "' and table_name='"
				+ table
				+ "' and column_name = '" + column + "'");
		rset.next();
		if (rset.getString(1).equals("1"))
			return true;
		return false;
	}

	public void makeGlobalTable() throws Exception {
		if (tableExists(testOwner, testTablePrefix + "GLOBAL"))
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
			sql += "union all select " + Joiner.on(", ").join(values)
					+ " from dual\n";
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
			executeQuery("insert into " + testTablePrefix + entry.getKey()
					+ " (" + Joiner.on(", ").join(entry.getValue())
					+ ") select " + Joiner.on(", ").join(combineTableColumn)
					+ " from " + testTablePrefix + "GLOBAL");
		}
	}

	public boolean compareTables() throws Exception {
		boolean result = true;
		for (Map.Entry<String, ArrayList<String>> entry : dbTablesColumns
				.entrySet()) {
			for (String column : entry.getValue()) {
				ResultSet rset = executeQuery("select count(*) from (select case  when substr("
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
						+ ") from "
						+ sourceOwner
						+ "."
						+ sourceTablePrefix
						+ entry.getKey() + ")");
				rset.next();
				if (rset.getString(1).equals("0")) {
					System.out.println("SUCCES " + entry.getKey() + "__"
							+ column + " data in source (with '.0' fix)");
				} else {
					result = false;
					System.out.println("FAILED datacompare for: "
							+ entry.getKey() + "__" + column);
				}

			}
		}
		return result;
	}

	@Test
	public void init() throws Exception {
		Locale.setDefault(Locale.US);
		getMatrixColumnsIndex();
		getDbTablesColumns();
	}

	@Test(dependsOnMethods = { "init" })
	public void testMakeGlobalTable() throws Exception {
		makeGlobalTable();
	}

	@Test(dependsOnMethods = { "testMakeGlobalTable" })
	public void testFillGlobalTable() throws Exception {
		fillGlobalTable();
	}

	@Test(dependsOnMethods = { "testFillGlobalTable" })
	public void testMakeTables() throws Exception {
		makeTables();
	}

	@Test(dependsOnMethods = { "testMakeTables" })
	public void testFillTables() throws Exception {
		fillTables();
	}

	@Test(dependsOnMethods = { "testFillTables" })
	public void testCompareTables() throws Exception {
		if (compareTables() == false)
			Assert.assertFalse(true);
	}

}
