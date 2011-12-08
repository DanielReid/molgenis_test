
package org.molgenis.test.ui;

import org.molgenis.framework.db.Database;
import org.molgenis.framework.ui.EasyPluginController;
import org.molgenis.framework.ui.ScreenController;
import org.molgenis.framework.ui.html.FlowLayout;
import org.molgenis.framework.ui.html.HtmlElement.UiToolkit;
import org.molgenis.framework.ui.html.ActionInput;
import org.molgenis.framework.ui.html.HtmlSettings;
import org.molgenis.framework.ui.html.LabelInput;
import org.molgenis.framework.ui.html.MolgenisForm;
import org.molgenis.framework.ui.html.SelectInput;
import org.molgenis.framework.ui.html.VerticalLayout;
import org.molgenis.util.Tuple;


/**
 * InputsTestController takes care of all user requests and application logic.
 *
 * <li>Each user request is handled by its own method based action=methodName. 
 * <li> MOLGENIS takes care of db.commits and catches exceptions to show to the user
 * <li>InputsTestModel holds application state and business logic on top of domain model. Get it via this.getModel()/setModel(..)
 * <li>InputsTestView holds the template to show the layout. Get/set it via this.getView()/setView(..).
 */
public class ChooseLibrary extends EasyPluginController<ChooseLibraryModel>
{
	private static final long serialVersionUID = 1L;

	public ChooseLibrary(String name, ScreenController<?> parent)
	{
		super(name, null, parent);
		this.setModel(new ChooseLibraryModel(this)); //the default model
		//this.setView(new FreemarkerView("InputsTestView.ftl", getModel())); //<plugin flavor="freemarker"
	}
	
	/**
	 * At each page view: reload data from database into model and/or change.
	 *
	 * Exceptions will be caught, logged and shown to the user automatically via setMessages().
	 * All db actions are within one transaction.
	 */ 
	@Override
	public void reload(Database db) throws Exception
	{	
//		//example: update model with data from the database
//		Query q = db.query(Investigation.class);
//		q.like("name", "molgenis");
//		getModel().investigations = q.find();
	}
	
	public void changelibrary(Database db, Tuple request)
	{
		logger.info("changelibrary: " + request);
		String lib = request.getString("library");
		if("DOJO".equals(lib)) HtmlSettings.uiToolkit = UiToolkit.DOJO;
		if("JQUERY".equals(lib)) HtmlSettings.uiToolkit = UiToolkit.JQUERY;
		if("DEFAULT".equals(lib)) HtmlSettings.uiToolkit = UiToolkit.ORIGINAL;
		
	}
	
	public String render()
	{
		MolgenisForm main = new MolgenisForm(this, new VerticalLayout());
		
		main.add(new LabelInput("select demo (and to change library used)"));
		
		FlowLayout libraryPanel = new FlowLayout();
		
		SelectInput select = new SelectInput("library", HtmlSettings.uiToolkit.toString());
		select.addOption("JQUERY", "Jquery toolkit");
		select.addOption("DEFAULT","MOLGENIS original");
		
		libraryPanel.add(select);
		
		libraryPanel.add(new ActionInput("changelibrary"));
		
		main.add(libraryPanel);
		
		return main.render();
	}
}