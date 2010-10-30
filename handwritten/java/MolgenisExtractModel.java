import org.molgenis.model.JDBCModelExtractor;

/**
 * If you configure your molgenis.properties to point an existing database,
 * this function will extracts a molgenis database (entity) model from it.*
 */
public class MolgenisExtractModel
{
	public static void main(String[] args) throws Exception
	{
		JDBCModelExtractor.main(args);
	}
}
