package molgenis.test.mref;

import java.io.File;

import org.molgenis.framework.db.Database;

import app.CsvImport;
import app.JDBCDatabase;

public class TestMrefImport
{
	public static void main(String[] args) throws Exception
	{
		File directory = new File(TestMrefImport.class.getResource("").getFile());
		System.out.println("Importing from dir "+directory);
		Database db = new JDBCDatabase("molgenis.properties");
		
		CsvImport.importAll(directory, db, null);

	

	}
}
