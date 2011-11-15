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

public class DataTestPublishStage {

	String studie;

	String dbDriver1;
	String dbUrl1;
	String dbUsername1;
	String dbPassword1;

	String dbDriver2;
	String dbUrl2;
	String dbUsername2;
	String dbPassword2;

	String publishOwnerPrefix;
	String stageOwnerPrefix;

	String[] excludedTables;
	String[] excludedColumns;

	Statement stmt1;
	Statement stmt2;
	ResultSet rset1;
	ResultSet rset2;

	Integer stid;

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
			Connection conn = DriverManager.getConnection(dbUrl2, dbUsername2,
					dbPassword2);
			stmt2 = conn.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void getPublishTablesColumns() throws Exception {
		String sql = "";
		sql += "select\n";
		sql += "  s.stid, pds.tabnaam, pds.veld\n";
		sql += "from\n";
		sql += "LLPOPER.studie s\n";
		sql += "  join\n";
		sql += "  LLPOPER.publ_dict_studie pds\n";
		sql += "  on s.stid = pds.stid\n";
		sql += "where s.studie = '" + studie + "'\n";
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
			}
		}
	}

	public Boolean compareTableColums() throws Exception {
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
				if (!rset2.getString(1).equals("1")) {
					System.out.println(tabCols.getKey() + " " + col);
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
		for (Map.Entry<String, ArrayList<String>> tabCols : publishTablesColumns
				.entrySet()) {
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
			sql += "  " + publishOwnerPrefix + tabCols.getKey() + "\n";
			rset1 = stmt1.executeQuery(sql);

			while (rset1.next()) {
				counter++;
				counterTotal++;
				String whereColVal = "";
				sql = "";
				sql += "select ";
				sql += "count(*) ";
				sql += "from ";
				sql += stageOwnerPrefix + tabCols.getKey() + " ";
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
				}
				if (counter >= 500)
				{
					counter = 0;
					System.out.println(counterTotal + " records processed.");		
				}
				rset2.close();
			}
			rset1.close();
		}
		return fail;
	}

}
