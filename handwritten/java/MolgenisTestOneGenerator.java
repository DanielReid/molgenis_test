import org.molgenis.Molgenis;
import org.molgenis.generators.DataTypeGen;
import org.molgenis.generators.csv.CsvReaderGen;
import org.molgenis.generators.db.JDBCMetaDatabaseGen;
import org.molgenis.generators.db.MultiqueryMapperGen;
import org.molgenis.generators.ui.FormScreenGen;
import org.molgenis.generators.ui.HtmlFormGen;
import org.molgenis.generators.ui.MenuScreenGen;
import org.molgenis.generators.ui.PluginScreenGen;


/**
 * This function will generate a molgenis, assuming a molgenis.properties file.
 */
public class MolgenisTestOneGenerator
{
	public static void main(String[] args) throws Exception
	{
		new Molgenis("molgenis.properties", CsvReaderGen.class, DataTypeGen.class, JDBCMetaDatabaseGen.class, MultiqueryMapperGen.class, FormScreenGen.class, MenuScreenGen.class, PluginScreenGen.class, HtmlFormGen.class).generate();
		//new Molgenis(DataTypeGen.class).generate();
		//new Molgenis(DataTypeGen.class, MultiqueryMapperGen.class).generate();
	}
}
