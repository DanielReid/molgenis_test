package test.web;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.google.common.base.Joiner;

public class DataTestMatrix {

	String dbDriver;
	String dbUrl;
	String dbUsername;
	String dbPassword;

	String file;

	String testTablePrefix;
	String testOwner;
	String sourceTablePrefix;
	String sourceOwner;
	String matrixSeperator;

	String filter1Table = "";
	String filter1Column = "";
	String filter1Operator = "";
	String filter1Value = "";

	String filter2Table = "";
	String filter2Column = "";
	String filter2Operator = "";
	String filter2Value = "";

	Statement stmt;
	ResultSet rset;
	String[] paidMatrixColumnNameParts;
	Map<Integer, String> matrixColumnsIndex = new HashMap<Integer, String>();
	Map<String, ArrayList<String>> publishTablesColumns = new HashMap<String, ArrayList<String>>();

	public void init() {
		Locale.setDefault(Locale.US);
		try {
			Class.forName(dbDriver);
			Connection conn = DriverManager.getConnection(dbUrl, dbUsername,
					dbPassword);
			stmt = conn.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void getMatrixColumnsIndex() throws Exception {
		System.out.println("Getting Matrix column index...");
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

	public Boolean getPA_ID() {
		for (String column : matrixColumnsIndex.values()) {
			if (column.contains("PA_ID")) {
				paidMatrixColumnNameParts = column.split("__");
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
		for (Map.Entry<Integer, String> matrixColumn : matrixColumnsIndex
				.entrySet()) {
			String[] matrixColumnParts = matrixColumn.getValue().split("__");
			if (matrixColumnParts.length == 2) {
				if (tableColumnExists(sourceOwner, sourceTablePrefix
						+ matrixColumnParts[0], matrixColumnParts[1])) {
					if (publishTablesColumns.get(matrixColumnParts[0]) == null) {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns.add(matrixColumnParts[1]);
						publishTablesColumns.put(matrixColumnParts[0],
								dbColumns);
					} else {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns = publishTablesColumns
								.get(matrixColumnParts[0]);
						if (dbColumns.contains(matrixColumnParts[1]) == false)
							dbColumns.add(matrixColumnParts[1]);
						publishTablesColumns.put(matrixColumnParts[0],
								dbColumns);
					}
				} else {
					System.out.println("FAIL: '" + sourceTablePrefix
							+ matrixColumnParts[0] + "." + matrixColumnParts[1]
							+ "' (based on matrix column) not in source");
					fail = true;
				}
			} else {
				if (matrixColumn.getValue().equals("")
						|| matrixColumn.getValue().equals("ROWNUMBER")) {
				} else {
					System.out
							.println("FAIL: '"
									+ matrixColumn.getValue()
									+ "' not conform format. (can not extract the source table and column name)");
					fail = true;
				}
			}
		}
		return fail;
	}

	boolean tableExists(String owner, String table) throws Exception {
		rset = stmt
				.executeQuery("select count(*) from dba_all_tables where owner='"
						+ owner + "' and table_name='" + table + "'");
		rset.next();
		if (rset.getString(1).equals("1"))
			return true;
		return false;
	}

	boolean tableColumnExists(String owner, String table, String column)
			throws Exception {
		rset = stmt
				.executeQuery("select count(*) from dba_tab_columns where owner='"
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
		System.out.println("Making global table...");
		if (tableExists(testOwner, testTablePrefix + "GLOBAL"))
			stmt.executeQuery("drop table " + testTablePrefix + "GLOBAL");
		stmt.executeQuery("create table "
				+ testTablePrefix
				+ "GLOBAL"
				+ " ("
				+ Joiner.on(" varchar(255), ")
						.join(matrixColumnsIndex.values()) + "  varchar(255))");
	}

	public void fillGlobalTable() throws Exception {
		System.out.println("Filling global table...");
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
		stmt.executeQuery(sql);
	}

	public void makeTables() throws Exception {
		System.out.println("Make tables...");
		for (Map.Entry<String, ArrayList<String>> entry : publishTablesColumns
				.entrySet()) {
			if (tableExists(testOwner, testTablePrefix + entry.getKey()))
				stmt.executeQuery("drop table " + testTablePrefix
						+ entry.getKey());
			stmt.executeQuery("create table " + testTablePrefix
					+ entry.getKey() + " ("
					+ Joiner.on(" varchar(255), ").join(entry.getValue())
					+ "  varchar(255))");
		}
	}

	public void fillTables() throws Exception {
		System.out.println("Filling tables...");
		ArrayList<String> combineTableColumn = new ArrayList<String>();
		for (Map.Entry<String, ArrayList<String>> entry : publishTablesColumns
				.entrySet()) {
			combineTableColumn.clear();
			for (String column : entry.getValue()) {
				if (paidMatrixColumnNameParts[1] == column)
					combineTableColumn.add(paidMatrixColumnNameParts[0] + "__"
							+ column);
				else
					combineTableColumn.add(entry.getKey() + "__" + column);
			}
			String sql = "insert into " + testTablePrefix + entry.getKey()
					+ " (" + Joiner.on(", ").join(entry.getValue())
					+ ") select " + Joiner.on(", ").join(combineTableColumn)
					+ " from " + testTablePrefix + "GLOBAL group by "
					+ Joiner.on(", ").join(combineTableColumn);
			stmt.executeQuery(sql);
		}
	}

	public boolean compareTables() throws Exception {
		boolean fail = false;
		for (Map.Entry<String, ArrayList<String>> entry : publishTablesColumns
				.entrySet()) {
			System.out.println("Comparing " + entry.getKey() + "...");
			for (String column : entry.getValue()) {
				String sql = "";

				String sqlTestTable = "select case  when substr(" + column
						+ ", length(" + column + ")-1, 2) = '.0'  then substr("
						+ column + ", 0, length(" + column + ")-2) else "
						+ column + " end as " + column + " from "
						+ testTablePrefix + entry.getKey() + " ";

				String sqlSourceTable = "select to_char(" + column + ") from "
						+ sourceOwner + "." + sourceTablePrefix
						+ entry.getKey() + " ";

				if (filter1Table.length() != 0
						&& filter1Table.equals(entry.getKey()))
					sqlSourceTable += "where " + filter1Column + " "
							+ filter1Operator + " '" + filter1Value + "' ";

				if (filter2Table.length() != 0
						&& filter2Table.equals(entry.getKey()))
					sqlSourceTable += "and " + filter2Column + " "
							+ filter2Operator + " '" + filter2Value + "' ";

				sql = "select count(*) from (" + sqlTestTable + " minus "
						+ sqlSourceTable + ")";

				rset = stmt.executeQuery(sql);
				rset.next();
				if (Integer.parseInt(rset.getString(1)) == 0) {
					System.out.println("SUCCES " + entry.getKey() + "__"
							+ column + " data in source (with '.0' fix)");
				} else {
					fail = true;
					System.out.println("FAILED datacompare for: "
							+ entry.getKey() + "__" + column);
				}

			}
		}
		return fail;
	}

}
