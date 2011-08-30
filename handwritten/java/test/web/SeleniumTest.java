package test.web;

import org.openqa.selenium.server.RemoteControlConfiguration;
import org.openqa.selenium.server.SeleniumServer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import webserver.boot.RunStandalone;
import webserver.test.Helper;
import com.thoughtworks.selenium.DefaultSelenium;
import com.thoughtworks.selenium.HttpCommandProcessor;

public class SeleniumTest
{

	DefaultSelenium selenium;
	String pageLoadTimeout = "30000";

	@BeforeClass
	public void start() throws Exception
	{

		int webserverPort = 8080;
		webserverPort = Helper.getAvailablePort(11000, 100);
		
		String seleniumUrl = "http://localhost:" + webserverPort + "/";
		String seleniumHost = "localhost";
		String seleniumBrowser = "firefox";
		int seleniumPort = Helper.getAvailablePort(9080, 100);
	
		RemoteControlConfiguration rcc = new RemoteControlConfiguration();
		rcc.setSingleWindow(true);
		rcc.setPort(seleniumPort);

		try
		{
			SeleniumServer server = new SeleniumServer(false, rcc);
			server.boot();
		}
		catch (Exception e)
		{
			throw new IllegalStateException("Cannot start selenium server: ", e);
		}

		HttpCommandProcessor proc = new HttpCommandProcessor(seleniumHost, seleniumPort, seleniumBrowser, seleniumUrl);
		selenium = new DefaultSelenium(proc);
		selenium.start();
		
		new RunStandalone(webserverPort);
		
	}
	
	@Test
	public void title() throws InterruptedException
	{
		selenium.open("/molgenis_test/molgenis.do");
		selenium.waitForPageToLoad(pageLoadTimeout);
		Assert.assertEquals(selenium.getTitle(), "My First MOLGENIS");
	}
	
	@Test(dependsOnMethods={"title"})
	public void clickIets() throws InterruptedException
	{
		selenium.click("id=Varchars2_tab_button");
		selenium.waitForPageToLoad(pageLoadTimeout);
		
		Assert.assertTrue(false);
		
	}
	
	@AfterClass
	public void stop() throws Exception
	{
		selenium.stop();
	}
	
}
