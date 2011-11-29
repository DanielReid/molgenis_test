package test.web;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class DataTestCompareTables {

	String studie;

	String dbDriverOracle;
	String dbUrlOracle;
	String dbUsernameOracle;
	String dbPasswordOracle;

	String dbDriverMSSQL;
	String dbUrlMSSQL;

	String[] excludedTables;
	String[] excludedColumns;

	Statement stmtOracle;
	Statement stmtMSSQL;
	ResultSet rsetOracle;
	ResultSet rsetMSSQL;

	Integer stid;
	Integer totalRecordCount = 0;

	Map<String, ArrayList<String>> publishCitTablesColumns = new HashMap<String, ArrayList<String>>();
	Map<String, ArrayList<String>> publishUmcgTablesColumns = new HashMap<String, ArrayList<String>>();

	public void init() {
		Locale.setDefault(Locale.US);
		try {
			Class.forName(dbDriverOracle);
			Connection conn = DriverManager.getConnection(dbUrlOracle, dbUsernameOracle,
					dbPasswordOracle);
			stmtOracle = conn.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
		try {
			Class.forName(dbDriverMSSQL);
			Connection conn = DriverManager.getConnection(dbUrlMSSQL);
			stmtMSSQL = conn.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void getPublishUmcgTablesColumns() throws Exception {
		System.out.println("");
		String sql = "";
		sql += "select TABLE_NAME, COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS";
		rsetMSSQL = stmtMSSQL.executeQuery(sql);
		while (rsetMSSQL.next()) {
			Boolean TableOrColumnNotExcluded = true;
			for (int i = 0; i < excludedTables.length; i++) {
				if (excludedTables[i].equals(rsetMSSQL.getString(1)))
					TableOrColumnNotExcluded = false;
			}
			for (int i = 0; i < excludedColumns.length; i++) {
				if (excludedColumns[i].equals(rsetMSSQL.getString(2)))
					TableOrColumnNotExcluded = false;
			}
			if (TableOrColumnNotExcluded) {
				if (publishUmcgTablesColumns.get(rsetMSSQL.getString(1)) == null) {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns.add(rsetMSSQL.getString(2));
					publishUmcgTablesColumns.put(rsetMSSQL.getString(1), dbColumns);
				} else {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns = publishUmcgTablesColumns
							.get(rsetMSSQL.getString(1));
					if (dbColumns.contains(rsetMSSQL.getString(2)) == false)
						dbColumns.add(rsetMSSQL.getString(2));
					publishUmcgTablesColumns.put(rsetMSSQL.getString(1), dbColumns);
				}
			} else {
				System.out
						.println("\tWARNING: ignoring table: "
								+ rsetMSSQL.getString(1) + " column: "
								+ rsetMSSQL.getString(2));
			}
		}
	}

	public boolean rowCountUmcgVsCit() throws Exception {
		System.out.println("");
		Boolean fail = false;
		for (Map.Entry<String, ArrayList<String>> tabColsUmcg : publishUmcgTablesColumns
				.entrySet()) {
			String sql = "select count(*) from " + tabColsUmcg.getKey();
			rsetOracle = stmtOracle.executeQuery(sql);
			rsetOracle.next();
			rsetMSSQL = stmtMSSQL.executeQuery(sql);
			rsetMSSQL.next();
			if (Integer.parseInt(rsetOracle.getString(1)) == Integer.parseInt(rsetMSSQL
					.getString(1))) {
				totalRecordCount += Integer.parseInt(rsetOracle.getString(1));
				System.out.println("Rowcount " + tabColsUmcg.getKey()
						+ " both: " + rsetOracle.getString(1) + ".");
			} else {
				fail = true;
				System.out.println("FAILED: Rowcount " + tabColsUmcg.getKey()
						+ " CIT: " + rsetOracle.getString(1) + ", UMCG: "
						+ rsetMSSQL.getString(1) + ".");
				
			}
		}
		return fail;
	}

	public boolean lookupTablesColumnsUmcgInCit() throws Exception {
		System.out.println("");
		Boolean fail = false;
		for (Map.Entry<String, ArrayList<String>> tabColsUmcg : publishUmcgTablesColumns
				.entrySet()) {
			for (String colUmcg : tabColsUmcg.getValue()) {
				String sql = "";
				sql += "select\n";
				sql += "  count(*)\n";
				sql += "from\n";
				sql += "  all_tab_columns atc\n";
				sql += "  left join all_synonyms s\n";
				sql += "  on (atc.owner = s.table_owner and atc.table_name = s.table_name)\n";
				sql += "where s.synonym_name = '" + tabColsUmcg.getKey()
						+ "'\n";
				sql += "and atc.column_name = '" + colUmcg + "'\n";
				rsetOracle = stmtOracle.executeQuery(sql);
				rsetOracle.next();
				if (Integer.parseInt(rsetOracle.getString(1)) == 1) {
					System.out.println("\tFound table: " + tabColsUmcg.getKey()
							+ " column: " + colUmcg);
				} else {
					fail = true;
					System.out.println("FAILED: table:" + tabColsUmcg.getKey()
							+ " column: " + colUmcg
							+ " does not exists in CIT Publish database.");
				}
			}
		}
		return fail;
	}

	public boolean lookupDataUmcgInCit() throws Exception {
		Boolean fail = false;
		Integer counter = 0;
		Integer counterTotal = 0;
		Integer counterFail = 0;
		String failMessage = "";
		for (Map.Entry<String, ArrayList<String>> tabCols : publishUmcgTablesColumns
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
			rsetMSSQL = stmtMSSQL.executeQuery(sql);
			while (rsetMSSQL.next()) {
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
					if (rsetMSSQL.getString(col) == null)
						whereColVal += " IS NULL ";
					else
						whereColVal += " = '" + rsetMSSQL.getString(col) + "' ";
				}
				sql += whereColVal;
				
				System.out.println(sql);
				
				rsetOracle = stmtOracle.executeQuery(sql);
				rsetOracle.next();
				if (rsetOracle.getString(1).equals("0")) {
					System.out.println("FAILED: " + sql);
					fail = true;
					failMessage = "PREVIOUSLY FAILED: ";
					counterFail++;
					if (counterFail >= 100) {
						System.out
								.println("FAIL COUNT IS >= 100, ENDING TEST...");
						return true;
					}
				}
				if (counter >= 500) {
					counter = 0;
					System.out.println(failMessage + counterTotal + "/"
							+ totalRecordCount + " records processed.");
				}
				rsetOracle.close();
			}
			rsetMSSQL.close();
		}
		return fail;
	}

}
