package webserver.core;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import webserver.generic.Utils;

public class WWWServer extends Webserver implements Runnable
{
	private static final long serialVersionUID = 1L;

	public WWWServer(String variant) throws IOException
	{
		Utils.console("Starting server");
		Webserver.PathTreeDictionary aliases = new Webserver.PathTreeDictionary();

		// Filesystem aliases
		aliases.put("/cgi-bin", new java.io.File("WebContent/cgi-bin"));
		aliases.put("/", new java.io.File("WebContent"));
		setMappingTable(aliases);
		
		// Serving all servlets in handwritten/java/servlets

		System.out.println("GetServlets starting");
		
		HashMap<String, String> autoMapping = new HashMap<String, String>();
		
		for (String key : autoMapping.keySet())	{
			addServlet(variant + "/" + key, autoMapping.get(key));
		}
		
		// Serving molgenis, API's, CGI, static files, tmp files
		addServlet(variant + "/molgenis.do", "app.servlet.MolgenisServlet");
		//addServlet(variant + "/api/R", "RApiServlet");
		addServlet(variant + "/api/find/", "app.servlet.MolgenisServlet");
		addServlet(variant + "/api", "app.servlet.MolgenisServlet");
		addServlet(variant + "/xref", "app.servlet.MolgenisServlet");
		addServlet(variant + "/cgi-bin", "webserver.core.servlets.CGIServlet");
		addServlet(variant + "/tmpfile", "webserver.core.servlets.tmpfileservlet");
		addServlet(variant + "/", "webserver.core.servlets.FileServlet");
		addServlet(variant + "/bot", "webserver.core.servlets.BotServlet");
	}

	public void run()
	{
		try
		{
			serve();
			return;
		}
		catch (Exception e)
		{
			log("ERROR [http server] ", e);
			e.printStackTrace();
			return;
		}
	}

}
