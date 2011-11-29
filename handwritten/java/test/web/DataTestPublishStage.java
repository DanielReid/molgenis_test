package test.web;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class DataTestPublishStage {

	String studie;

	String dbDriver1;
	String dbUrl1;
	String dbUsername1;
	String dbPassword1;

	String dbDriver2;
	String dbUrl2;

	String[] excludedTables;
	String[] excludedColumns;

	Statement stmt1;
	Statement stmt2;
	ResultSet rset1;
	ResultSet rset2;

	Integer stid;
	Integer totalRecordCount = 0;

	Map<String, ArrayList<String>> publishTablesColumns = new HashMap<String, ArrayList<String>>();

	public void init() {
		Locale.setDefault(Locale.US);
		try {
			Class.forName(dbDriver1);
			Connection conn = DriverManager.getConnection(dbUrl1, dbUsername1,
					dbPassword1);
			stmt1 = conn.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
		try {
			Class.forName(dbDriver2);
			Connection conn = DriverManager.getConnection(dbUrl2);
			stmt2 = conn.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void getPublishTablesColumns() throws Exception {
		System.out
				.println("Getting all the related tables form publ_dict_studie...");
		String sql = "";
		sql += "select\n";
		sql += "  0, tabnaam, veld\n";
		sql += "from\n";
		sql += "  vw_dict\n";
		sql += "  group by tabnaam, veld";
		rset1 = stmt1.executeQuery(sql);
		while (rset1.next()) {
			Boolean TableOrColumnNotExcluded = true;
			for (int i = 0; i < excludedTables.length; i++) {
				if (excludedTables[i].equals(rset1.getString(2)))
					TableOrColumnNotExcluded = false;
			}
			for (int i = 0; i < excludedColumns.length; i++) {
				if (excludedColumns[i].equals(rset1.getString(3)))
					TableOrColumnNotExcluded = false;
			}
			if (TableOrColumnNotExcluded) {
				stid = Integer.parseInt(rset1.getString(1));
				if (publishTablesColumns.get(rset1.getString(2)) == null) {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns.add(rset1.getString(3));
					publishTablesColumns.put(rset1.getString(2), dbColumns);
				} else {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns = publishTablesColumns.get(rset1.getString(2));
					if (dbColumns.contains(rset1.getString(3)) == false)
						dbColumns.add(rset1.getString(3));
					publishTablesColumns.put(rset1.getString(2), dbColumns);
				}
			} else {
				System.out
						.println("\tWARNING: ignoring table: "
								+ rset1.getString(2) + " column: "
								+ rset1.getString(3));
			}
		}
	}

	public void getTotalRecordCount() throws Exception {
		for (Map.Entry<String, ArrayList<String>> tabCols : publishTablesColumns
				.entrySet()) {
			rset1 = stmt1.executeQuery("select count(*) from "
					+ tabCols.getKey());
			rset1.next();
			totalRecordCount += Integer.parseInt(rset1.getString(1));
		}
	}

	public Boolean compareTableColums() throws Exception {
		System.out
				.println("Starting table and column lookup in the Stage database...");
		Boolean fail = false;
		for (Map.Entry<String, ArrayList<String>> tabCols : publishTablesColumns
				.entrySet()) {
			for (String col : tabCols.getValue()) {
				String sql = "";
				sql += "select \n";
				sql += "  count(*) \n";
				sql += "from \n";
				sql += "  INFORMATION_SCHEMA.COLUMNS \n";
				sql += "where \n";
				sql += "  TABLE_NAME = '" + tabCols.getKey() + "' \n";
				sql += "  and COLUMN_NAME = '" + col + "' \n";
				rset2 = stmt2.executeQuery(sql);
				rset2.next();
				if (Integer.parseInt(rset2.getString(1)) == 1) {
					System.out.println("\tFound table: " + tabCols.getKey()
							+ " column: " + col);
				} else {
					System.out.println("\tFAILED: Not found table: "
							+ tabCols.getKey() + " column: " + col);
					fail = true;
				}
			}
		}
		return fail;
	}

	public Boolean compareData() throws Exception {
		Boolean fail = false;
		Integer counter = 0;
		Integer counterTotal = 0;
		Integer counterFail = 0;
		String failMessage = "";
		for (Map.Entry<String, ArrayList<String>> tabCols : publishTablesColumns
				.entrySet()) {
			System.out.println("Starting with: " + tabCols);
			String sql = "";
			String selectCol = "";
			sql += "select\n";
			for (String col : tabCols.getValue()) {
				if (selectCol.length() == 0)
					selectCol += "  ";
				else
					selectCol += "  ,";
				selectCol += col + "\n";
			}
			sql += selectCol;
			sql += "from\n";
			sql += "  " + tabCols.getKey() + "\n";
			rset1 = stmt1.executeQuery(sql);
			while (rset1.next()) {
				counter++;
				counterTotal++;
				String whereColVal = "";
				sql = "";
				sql += "select ";
				sql += "count(*) ";
				sql += "from ";
				sql += tabCols.getKey() + " ";
				sql += "where ";
				for (String col : tabCols.getValue()) {
					if (whereColVal.length() != 0)
						whereColVal += "and ";
					whereColVal += col;
					if (rset1.getString(col) == null)
						whereColVal += " IS NULL ";
					else
						whereColVal += " = '" + rset1.getString(col) + "' ";
				}
				sql += whereColVal;
				rset2 = stmt2.executeQuery(sql);
				rset2.next();
				if (rset2.getString(1).equals("0")) {
					System.out.println("FAILED: " + sql);
					fail = true;
					failMessage = "PREVIOUSLY FAILED: ";
					counterFail++;
					if (counterFail >= 100)
					{
						System.out.println("FAIL COUNT IS >= 100, ENDING TEST...");
						return true;
					}
				}
				if (counter >= 500) {
					counter = 0;
					System.out.println(failMessage + counterTotal + "/"
							+ totalRecordCount + " records processed.");
				}
				rset2.close();
			}
			rset1.close();
		}
		return fail;
	}

}
