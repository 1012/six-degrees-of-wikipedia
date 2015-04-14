package be.casisto.tinkering.wikipedia;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line interface implementation for the Wikipedia indexer tool.
 * 
 * @author jiri
 */
public class IndexerCLI {

	private static final Logger log = LoggerFactory
			.getLogger(IndexerTool.class);

	private String[] args = null;
	private Options options = new Options();

	private Configuration config = new Configuration();
	private FileSystem fs = null;

	private String inputFile;
	private String outputDir;

	private boolean cleanup = false;

	/**
	 * Main method for the IndexerCLI.
	 * 
	 * -c,--cleanup Cleanup HDFS after run by deleting the input, pages and
	 * links files for this iteration. -f,--hdfs <arg> Hadoop file system url.
	 * Default is local hdfs with default port (hdfs://localhost:9000).
	 * -h,--help Show this. -i,--input <arg> The unzipped wikipedia xml to
	 * index. -o,--output <arg> The output destination for the index files
	 * -t,--tracker <arg> Hadoop job tracker url. Default is local hadoop with
	 * default port (localhost:9010).
	 *
	 * @param args
	 *            command-line arguments
	 */
	public static void main(String[] args) {
		IndexerCLI cli = new IndexerCLI(args);
		cli.parseArgs();
		cli.run();
	}

	/**
	 * Default constructor responsible for initialising the command-line parser.
	 * 
	 * @param args
	 *            command-line arguments
	 */
	public IndexerCLI(String[] args) {
		this.args = args;

		options.addOption("i", "input", true, "The wikipedia archive to index.");

		options.addOption("o", "output", true,
				"The output destination for the index files");

		options.addOption(
				"c",
				"cleanup",
				false,
				"Cleanup HDFS after run by deleting the input, pages and links files for this iteration.");

		options.addOption(
				"t",
				"tracker",
				true,
				"Hadoop job tracker url. Default is local hadoop with default port (localhost:9010).");

		options.addOption(
				"f",
				"hdfs",
				true,
				"Hadoop file system url. Default is local hdfs with default port (hdfs://localhost:9000).");

		options.addOption("h", "help", false, "Show this.");
	}

	/**
	 * Parse the command-line arguments and set default values where applicable.
	 */
	private void parseArgs() {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {

			cmd = parser.parse(options, args);

			if (cmd.hasOption("h"))
				help();

			if (cmd.hasOption("i"))
				inputFile = cmd.getOptionValue("i");
			else {
				log.error("No wikipedia archive provided as input!");
				help();
			}

			if (cmd.hasOption("o"))
				outputDir = cmd.getOptionValue("o");
			else {
				log.error("No output destination directory specified for the index files!");
				help();
			}

			if (cmd.hasOption("c"))
				cleanup = true;

			if (cmd.hasOption("t"))
				config.set("mapred.job.tracker", cmd.getOptionValue("t"));
			else
				config.set("mapred.job.tracker", "localhost:9010");

			if (cmd.hasOption("f"))
				config.set("fs.defaultFS", cmd.getOptionValue("f"));
			else
				config.set("fs.defaultFS", "hdfs://localhost:9000");

		} catch (ParseException e) {
			log.error("Failed to parse command-line", e);
			help();
		}
	}

	/**
	 * Run the wikipedia indexing jobs for pages and links on Hadoop.
	 */
	private void run() {

		String iteration = Long.toString(System.currentTimeMillis());
		String input = "wiki/input/input-" + iteration + ".txt";
		String links = "wiki/links/links-index-" + iteration;
		String pages = "wiki/pages/pages-index-" + iteration;

		try {
			log.info("Starting the wikipedia indexer.");

			log.info("Uploading wikipedia archive from {} to HDFS", inputFile);
			fs = FileSystem.get(config);
			this.upload(inputFile, input);

			log.info("Running the indexer");
			config.set("wiki.indexer.input",
					input.substring(0, input.lastIndexOf("/")));
			config.set("wiki.indexer.links", links);
			config.set("wiki.indexer.pages", pages);
			int res = ToolRunner.run(config, new IndexerTool(), args);

			if (res == 0)
				log.info("Indexer jobs finished successfully");
			else
				log.error("Indexer jobs exited with status code {}", res);

			log.info("Downloading index files from HDFS to {}", new File(
					outputDir).getAbsolutePath());
			this.download(pages + "/part-r-00000", outputDir + "/pages-index-"
					+ iteration + ".txt");
			this.download(links + "/part-r-00000", outputDir + "/links-index-"
					+ iteration + ".txt");

			if (cleanup) {
				log.info("Cleanup required. Deleting input, pages and links files from HDFS.");
				this.delete(input, false);
				this.delete(pages, true);
				this.delete(links, true);
			}

		} catch (Exception e) {
			log.error("Wikipedia indexer exited with an exception.", e);
		}

		log.info("Wikipedia indexer finished.");

	}

	/**
	 * Uploads a file from the local file system to HDFS.
	 * 
	 * @param localSource
	 *            local file
	 * @param remoteDestination
	 *            remote destination
	 * @throws IOException
	 */
	private void upload(String localSource, String remoteDestination)
			throws IOException {
		File localFile = new File(localSource);
		Path remote = new Path(remoteDestination);

		fs.copyFromLocalFile(new Path(localFile.getAbsolutePath()), remote);
	}

	/**
	 * Downloads a file from HDFS to the local file system.
	 * 
	 * @param remoteSource
	 *            remote file
	 * @param localDestination
	 *            local destination
	 * @throws IOException
	 */
	private void download(String remoteSource, String localDestination)
			throws IOException {
		File localFile = new File(localDestination);
		Path localPath = new Path(localFile.getAbsolutePath());
		Path remote = new Path(remoteSource);

		fs.copyToLocalFile(remote, localPath);
	}

	/**
	 * Deletes a file or directory from HDFS.
	 * 
	 * @param remote
	 * @param recursive
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void delete(String remote, boolean recursive)
			throws IllegalArgumentException, IOException {
		fs.delete(new Path(remote), recursive);
	}

	/**
	 * Prints the command-line help menu.
	 */
	private void help() {
		HelpFormatter formatter = new HelpFormatter();

		formatter.printHelp("IndexerCLI", options);
		System.exit(0);
	}

}
