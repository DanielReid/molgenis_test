package test.csv;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.molgenis.model.MolgenisModelException;
import  org.molgenis.model.elements.Entity;

import molgenis_test.fields.VarcharEntity;
import app.CsvExport;

public class CsvWriterTests
{
	public void testNullFields() throws IOException, MolgenisModelException
	{
		List<? extends Entity> vlist = new ArrayList();
		for(int i = 0; i < 3; i++)
		{
			VarcharEntity e = new VarcharEntity();
			e.setNormalVarchar("test"+i);
			e.setReadonlyVarchar("test"+i);
		}
		
		new CsvExport().exportVarcharEntity(vlist, new File("data/testcsv.txt"));
		
		
	}
}
