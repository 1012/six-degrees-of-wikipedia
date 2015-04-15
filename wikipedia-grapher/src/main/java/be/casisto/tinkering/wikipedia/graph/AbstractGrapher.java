package be.casisto.tinkering.wikipedia.graph;

import java.io.Closeable;

/**
 * Base class for the grapher implementation. 
 * 
 * IDEA: add Neo4J and TitanDB implementations to compare.
 * 
 * @author jiri
 */
public abstract class AbstractGrapher implements Closeable {
	
	public abstract void connect(String url, String username, String password);
	
	public abstract void close();
	
	public abstract boolean loadVertices(String file);
	
	public abstract boolean loadEdges(String file);
	
}
