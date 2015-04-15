package be.casisto.tinkering.wikipedia.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class OrientDBGrapher extends AbstractGrapher {

	private static Logger log = LoggerFactory.getLogger(OrientDBGrapher.class);

	private OrientGraphNoTx graph = null;
	
	@Override
	public void connect(String url, String username, String password) {

		if (graph == null || graph.isClosed()) {
			
			OrientGraphFactory factory = new OrientGraphFactory(url, username,
					password);
			graph = factory.getNoTx();
			
			graph.declareIntent(new OIntentMassiveInsert());
			
			Set<String> keys = graph.getIndexedKeys(Vertex.class);
			if (!keys.contains("title"))
				graph.createKeyIndex("title", Vertex.class);
			
		} else
			log.warn("Dabatase connection to {} already open!", url);

	}

	@Override
	public void close() {
		if (graph != null && !graph.isClosed()) {
			log.info("Closing database connection.");
			graph.shutdown();
			graph = null;
		} else
			log.warn("Database connection not open. Cannot close.");
	}

	@Override
	public boolean loadVertices(String file) {
		
		try {
			
			BufferedReader br = getFileReader(file);
			
			long count = 0;
			String line = br.readLine();
			while (line != null) {
				getVertex(line.trim());
				
				if (++count % 25000 == 0)
					log.info("Processed {} page vertices", count);
				
				line = br.readLine();
			}
			
			log.info("Processed a total of {} page vertices", count);
			
		}
		catch(FileNotFoundException e) {
			log.error("Unable to open page index file at {}!", file);
			return false;
		} catch (IOException e) {
			log.error("Unable to read line from page index file at {}!", file);
			return false;
		}
		
		return true;
		
	}

	@Override
	public boolean loadEdges(String file) {
		
		try {
			
			BufferedReader br = getFileReader(file);
			
			long count = 0;
			String line = br.readLine();
			while (line != null) {
				
				String[] parts = line.split("\t");
				
				String title = parts[0];
				String[] links = parts[1].split(",");
				
				for (String link: links) {
					
					createEdge(title, link, "links_to");
					
					if (++count % 25000 == 0)
						log.info("Processed {} page edges", count);
					
				}
				
				line = br.readLine();
				
			}
			
			log.info("Processed a total of {} page edges", count);
			
		}
		catch(FileNotFoundException e) {
			log.error("Unable to open link index file at {}!", file);
			return false;
		} catch (IOException e) {
			log.error("Unable to read line from link index file at {}!", file);
			return false;
		}
		
		return true;
		
	}
	
	private Vertex getVertex(String title) {
		Iterable<Vertex> pages = graph.getVertices("title", title);
		
		Vertex page = null;
		if (!pages.iterator().hasNext()) {
			page = graph.addVertex(null);
			page.setProperty("title", title);
		}
		else
			page = pages.iterator().next();

		return page;
	}
	
	private Edge createEdge(String source, String target, String label) {
		
		Vertex srcPage = getVertex(source);
		Vertex tgtPage = getVertex(target);
		
		Edge link = null;
		boolean found = false;
		
		Iterable<Edge> edges = srcPage.getEdges(Direction.OUT, "class:Link");
		for (Edge edge : edges) {
			Vertex vertex = edge.getVertex(Direction.OUT);
			
			String title = vertex.getProperty("title");
			
			if (title.equals(target)) {
				found = true;
				continue;
			}
		}
		
		if (!found)
			link = graph.addEdge(null, srcPage, tgtPage, "links to");
		
		return link;
	}
	
	private BufferedReader getFileReader(String file) throws FileNotFoundException {
		InputStream in = new FileInputStream(new File(file));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		return br;
	}

}
