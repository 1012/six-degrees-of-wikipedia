package be.casisto.tinkering.wikipedia;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.casisto.tinkering.wikipedia.graph.AbstractGrapher;

public class GrapherCLI {

	private static Logger log = LoggerFactory.getLogger(GrapherCLI.class);

	private String[] args = null;
	private Options options = new Options();

	private String pages;
	private String links;
	private String url = "remote:localhost/wikipedia";
	private String username = "admin";
	private String password = "admin";
	private String graphdb = "be.casisto.tinkering.wikipedia.graph.OrientDBGrapher";

	/**
	 * Main method.
	 * 
	 * @param args
	 *            command-line arguments
	 */
	public static void main(String[] args) {
		GrapherCLI cli = new GrapherCLI(args);
		cli.parseArgs();
		cli.run();
	}

	/**
	 * Default constructor that initializes the GrapherCLI instance.
	 * 
	 * @param args
	 *            command-line arguments
	 */
	public GrapherCLI(String[] args) {
		this.args = args;

		options.addOption("p", "pages", true,
				"The index of wikipedia pages resulting from wikipedia-indexer.");

		options.addOption("l", "links", true,
				"The index of wikipedia links between pages resulting from wikipedia-indexer.");

		options.addOption("db", "database", true,
				"The url of the OrientDB database (cluster).");

		options.addOption("u", "user", true,
				"User name for connecting to OrientDB.");

		options.addOption("pw", "password", true,
				"Password for connecting to OrientDB.");

		options.addOption(
				"g",
				"grapher",
				true,
				"Class name for the grapher implementation. Needs to extend AbstractGrapher. Default is the OrientDBGrapher.");

		options.addOption("h", "help", true, "Show help.");
	}

	/**
	 * Prints the command-line help menu.
	 */
	private void help() {
		HelpFormatter formatter = new HelpFormatter();

		formatter.printHelp("GrapherCLI", options);
		System.exit(0);
	}

	/**
	 * Method to parse and validate the command-line arguments.
	 */
	private void parseArgs() {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);

			if (cmd.hasOption("p"))
				pages = cmd.getOptionValue("p");
			else {
				log.error("You need to specify a pages index file!");
				help();
			}

			if (cmd.hasOption("l"))
				links = cmd.getOptionValue("l");
			else {
				log.error("You need to specify a links index file!");
				help();
			}

			if (cmd.hasOption("db"))
				url = cmd.getOptionValue("db");

			if (cmd.hasOption("u"))
				username = cmd.getOptionValue("u");

			if (cmd.hasOption("pw"))
				password = cmd.getOptionValue("pw");

			if (cmd.hasOption("g"))
				graphdb = cmd.getOptionValue("g");

		} catch (ParseException e) {
			log.error("Failed to parse command line!", e);
			help();
		}

	}

	/**
	 * Execute the graphing logic.
	 */
	private void run() {

		AbstractGrapher grapher = null;
		boolean connected = false;

		try {

			log.info("Initializing Wikipedia grapher {}", graphdb);
			grapher = (AbstractGrapher) Class.forName(graphdb).newInstance();

			log.info("Connecting to the graph database with url {}", url);
			grapher.connect(url, username, password);

			connected = true;

		} catch (InstantiationException e) {
			log.error("Failed to instantiate an instance of class {}!", graphdb);
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			log.error("Class {} not found!", graphdb);
			e.printStackTrace();
		}

		if (grapher != null && connected) {

			log.info("Loading page titles from {} as vertices", pages);
			grapher.loadVertices(pages);

			log.info("loading page links from {} as edges", links);
			grapher.loadEdges(links);

		}

		if (connected) {
			log.info("Disconnecting from the graph database");
			grapher.close();
		}

	}

}
