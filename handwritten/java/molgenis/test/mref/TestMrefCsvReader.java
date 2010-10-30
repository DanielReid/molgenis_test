package molgenis.test.mref;

import java.io.File;
import java.net.URL;

import molgenis_test.fields.MrefEntity;

import org.molgenis.util.CsvFileReader;
import org.molgenis.util.CsvReader;
import org.molgenis.util.CsvReaderListener;
import org.molgenis.util.Tuple;

public class TestMrefCsvReader
{
	public static void main(String[] args) throws Exception
	{
		URL f = TestMrefCsvReader.class.getResource("data.tab");
		CsvReader r = new CsvFileReader(new File(f.getFile()));

		r.parse(new CsvReaderListener()
		{

			@Override
			public void handleLine(int line_number, Tuple tuple) throws Exception
			{
				// TODO Auto-generated method stub
				System.out.println(tuple);
				if (tuple.notNull("mref_field")) for (Object o : tuple.getList("mref_field"))
				{
					
					System.out.println(o);
				}
				
				MrefEntity e = new MrefEntity();
				e.set(tuple);
				System.out.println(e);

			}
		});

	}
}
