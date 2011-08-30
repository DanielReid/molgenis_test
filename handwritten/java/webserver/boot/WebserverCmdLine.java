package webserver.boot;

import java.io.IOException;

import org.molgenis.MolgenisOptions;

import webserver.core.WWWServer;
import app.servlet.MolgenisServlet;
import app.servlet.UsedMolgenisOptions;

public class WebserverCmdLine implements Runnable{
	WWWServer web;
	static Thread webserverthread;

	MolgenisOptions usedOptions = new UsedMolgenisOptions();
	String variant = MolgenisServlet.getMolgenisVariantID();
	String title =  variant + " powered by Molgenis Webserver";
	final String url = "http://localhost:" + WWWServer.DEF_PORT + "/" + variant;

	WebserverCmdLine(Integer port) throws IOException{
		if(port != null){ WWWServer.DEF_PORT = port; }
	    (webserverthread = new Thread(web = new WWWServer(variant))).start();
	}

	@Override
	public void run() {
		int k;
		try {
			while(true){
				if((k = System.in.read()) >-1){
					System.out.println(k);
				}
				if(k==113){
					if(webserverthread.isAlive()){
						web.shutdown();
						try {
							web.getAcceptor().destroy();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
						web.getThreadPool().setMaxThreads(-1);
					}
					break;
				}
				Thread.sleep(20);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
