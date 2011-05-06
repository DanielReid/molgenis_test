/* Date:        March 9, 2009
 * Template:	PluginScreenJavaTemplateGen.java.ftl
 * generator:   org.molgenis.generate.screen.PluginScreenJavaTemplateGen 3.2.0-testing
 * 
 * THIS FILE IS A TEMPLATE. PLEASE EDIT :-)
 */

package org.molgenis.plugins.examples.file;

import java.util.ArrayList;
import java.util.List;

import molgenis_test.fields.FileEntity;

import org.molgenis.framework.db.Database;
import org.molgenis.framework.db.DatabaseException;
import org.molgenis.framework.ui.PluginModel;
import org.molgenis.framework.ui.ScreenController;
import org.molgenis.framework.ui.ScreenMessage;
import org.molgenis.util.Tuple;

public class DownloadPlugin extends PluginModel
{
	List<FileEntity> fileEntities = new ArrayList<FileEntity>();
	
	public List<FileEntity> getFileEntities()
	{
		return fileEntities;
	}

	public void setFileEntities(List<FileEntity> fileEntities)
	{
		this.fileEntities = fileEntities;
	}

	public DownloadPlugin(String name, ScreenController<?> parent)
	{
		super(name, parent);
	}

	@Override
	public String getViewName()
	{
		return "org_molgenis_plugins_examples_file_DownloadPlugin";
	}

	@Override
	public String getViewTemplate()
	{
		return "org/molgenis/plugins/examples/file/DownloadPlugin.ftl";
	}

	@Override
	public void reload(Database db)
	{
		try
		{
			//get all file entities form database
			this.fileEntities = db.find(FileEntity.class);
		}
		catch (DatabaseException e)
		{
			this.setMessages(new ScreenMessage("error: "+e, false));
			e.printStackTrace();
		}
	}

	@Override
	public boolean isVisible()
	{
		return true;
	}

	@Override
	public void handleRequest(Database db, Tuple request)
	{
		// TODO Auto-generated method stub	
	}
}
