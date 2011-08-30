package webserver.boot;

import java.awt.Color;
import java.awt.Event;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.io.IOException;

import javax.swing.JFrame;
import javax.swing.JTextArea;

import org.molgenis.MolgenisOptions;

import webserver.core.WWWServer;
import webserver.generic.OpenBrowser;
import webserver.generic.Utils;
import webserver.gui.Button;
import app.servlet.MolgenisServlet;
import app.servlet.UsedMolgenisOptions;

public class WebserverGui extends JFrame implements MouseListener{
	private static final long serialVersionUID = -4617569886547354084L;
	JTextArea output;
	WWWServer web;
	static Thread webserverthread;
	MolgenisOptions usedOptions = new UsedMolgenisOptions();
	OpenBrowser browser = new OpenBrowser();
	String variant = MolgenisServlet.getMolgenisVariantID();
	String title =  variant + " powered by Molgenis Webserver";
	final String url;
	boolean init=false;

	WebserverGui(Integer port) throws IOException{
		setSize(300,150); 
		setTitle(title);
		setDefaultCloseOperation(EXIT_ON_CLOSE);
	    setVisible(true);
	    addMouseListener(this);
	    if(port != null){ WWWServer.DEF_PORT = port; }
	    (webserverthread = new Thread(web = new WWWServer(variant))).start();
		url = "http://localhost:" + WWWServer.DEF_PORT + "/" + variant + "/molgenis.do";
		init = true;
		this.repaint();
	}
	
	public void paint(Graphics g) {
		paintComponent(g);
	}
	
	protected void clear(Graphics g) {
		super.paintComponents(g);
	}
	
	public void paintComponent(Graphics g) {
		Graphics2D g2d = (Graphics2D)g;
		
		g2d.setColor(Color.WHITE);
		g2d.fillRect(0, 0, getWidth(),getHeight());
		
		//CLICK to Molgenis
		if(init){
			
			g2d.setColor(Color.BLACK);
			g2d.drawString("Please open a browser and go to:",25,50);
			
			Button.render(url, 25, 60, g2d);

			//STOP
			if(webserverthread != null && webserverthread.isAlive()){
			Button.render("Stop", 90, 85, g2d);	
			
			}else{
			//START
			Button.render("Start", 130, 85, g2d);
			}
			Button.render("Restart", 170, 85, g2d);
			
			g2d.setColor(Color.BLACK);
			g2d.drawString("Webserver status:",35,120);
			
			if(webserverthread != null && webserverthread.isAlive()){
				g2d.setColor(Color.GREEN);
				g2d.drawString("Online",25,140);
				
			}else{
				g2d.setColor(Color.RED);
				g2d.drawString("Down",25,140);
				
			}
		}else{
			g2d.setColor(Color.BLACK);
			g2d.drawString("Webserver starting, please wait...",25,50);
		}
	}
	
	void startWebServer(){
	    try {
			web = new WWWServer(variant);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	    webserverthread = new Thread(web);
	    webserverthread.start();
	}

	void stopWebServer(){
		web.shutdown();
		try {
			web.getAcceptor().destroy();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		web.getThreadPool().setMaxThreads(-1);
	}

	@Override
	public void mouseClicked(MouseEvent e) {
		Utils.console(""+e.getX()+" "+e.getY());
		if(e.getX() > 25 && e.getX() < 225){
			if(e.getY() > 60 && e.getY() < 80){
				Utils.console("GOTO Molgenis clicked");
				browser.openURL(url);
			}
		}
		if(e.getY() > 85 && e.getY() < 105){
			if(e.getX() > 90 && e.getX() < 125){
				Utils.console("STOP webserver clicked");
				if(webserverthread.isAlive()){
					stopWebServer();
				}
				
			}else if(e.getX() > 130 && e.getX() < 170){
				Utils.console("START webserver clicked");
				if(!webserverthread.isAlive()){
					startWebServer();
				}
			}else if(e.getX() > 170 && e.getX() < 220){
				Utils.console("RESTART webserver clicked");
				if(webserverthread.isAlive()){
				    stopWebServer();
				}
				startWebServer();
			}
		}
		repaint();
	}

	@Override
	public void mouseEntered(MouseEvent e) {
		repaint();
	}
	
	@Override
	public boolean mouseMove(Event e, int x,int y) {
		repaint();
		return true;
	}

	@Override
	public void mouseExited(MouseEvent e) {
		repaint();
	}

	@Override
	public void mousePressed(MouseEvent e) {
		repaint();
	}

	@Override
	public void mouseReleased(MouseEvent e) {
		repaint();
	}

}
