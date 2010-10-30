///* Date:        December 3, 2008
// * Template:	PluginScreenJavaTemplateGen.java.ftl
// * generator:   org.molgenis.generate.screen.PluginScreenJavaTemplateGen 3.0.3
// * 
// * THIS FILE IS A TEMPLATE. PLEASE EDIT :-)
// */
//
//package plugin.login;
//
//import java.security.Security;
//
//import org.molgenis.framework.db.Database;
//import org.molgenis.framework.ui.PluginModel;
//import org.molgenis.framework.ui.ScreenMessage;
//import org.molgenis.framework.ui.ScreenModel;
//import org.molgenis.framework.ui.html.ActionInput;
//import org.molgenis.framework.ui.html.Form;
//import org.molgenis.framework.ui.html.PasswordInput;
//import org.molgenis.framework.ui.html.StringInput;
//import org.molgenis.util.Tuple;
//
///**
// * This screen shows a login box, or if someone is already logged in, the user
// * information and a logout button.
// */
//public class UserLogin extends PluginModel
//{
//	Security security;
//
//	public UserLogin(String name, ScreenModel parent)
//	{
//		super(name, parent);
//		security = this.getRootScreen().getLogin();
//	}
//
//	@Override
//	public String getViewName()
//	{
//		return "plugins_auth_UserLogin";
//	}
//
//	@Override
//	public String getViewTemplate()
//	{
//		return "plugin/login/UserLogin.ftl";
//	}
//
//	@Override
//	public void handleRequest(Database db, Tuple request)
//	{
//		// reset messages
//		this.setMessages();
//
//		logger.debug(request);
//		if ("Login".equals(request.getAction()))
//		{
//			boolean loggedIn = getLogin().login(db, request.getString("name"), request.getString("password"));
//			if (!loggedIn) 
//				this.setMessages(new ScreenMessage("login failed: username or password unknown", false));
//			else
//				this.getRootScreen().setLogin(security);
//		}
//		else if ("Logout".equals(request.getAction()))
//		{
//			getLogin().logout();
//		}
//	}
//
//	@Override
//	public void reload(Database db)
//	{
//		// nothing todo, Login takes care of this.
//	}
//
//	// DUMMY FOR TESTING
//	@Override
//	public Security getLogin()
//	{
//		return security;
//	}
//
//	public Form getInputs()
//	{
//		Form f = new Form();
//
//		if (getLogin().isAuthenticated())
//		{
//			f.add(new ActionInput("Logout"));
//		}
//		else
//		{
//			f.add(new ActionInput("Login"));
//			f.add(new PasswordInput("password"));
//			f.add(new StringInput("name"));
//		}
//
//		return f;
//	}
//
//	@Override
//	public boolean isVisible()
//	{
//		// TODO Auto-generated method stub
//		return true;
//	}
//	
//	@Override
//	public String getLabel()
//	{
//		if(getLogin().isAuthenticated())
//			return "Welcome '"+getLogin().getUserName()+"'";
//		return "Login";
//	}
//}
