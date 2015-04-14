package be.casisto.tinkering.wikipedia;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.casisto.tinkering.wikipedia.indexer.PageLinkMapper;
import be.casisto.tinkering.wikipedia.indexer.PageLinkReducer;
import be.casisto.tinkering.wikipedia.indexer.PageTitleMapper;
import be.casisto.tinkering.wikipedia.indexer.PageTitleReducer;
import be.casisto.tinkering.wikipedia.indexer.WikipediaInputFormat;

/**
 * Hadoop 2.0 tool implementation for executing and chaining the indexing jobs.
 * 
 * @author jiri
 */
public class IndexerTool extends Configured implements Tool {

	private static final Logger log = LoggerFactory
			.getLogger(IndexerTool.class);

	/**
	 * Executes the tool logic.
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		try {
			String input = this.getConf().get("wiki.indexer.input");
			String links = this.getConf().get("wiki.indexer.links");
			String pages = this.getConf().get("wiki.indexer.pages");

			log.info("Indexing wiki links");
			boolean indexed = indexWikiLinks(input, links);
			if (!indexed) {
				log.error("Failed to complete indexing of wiki links");
				System.exit(1);
			}

			log.info("Indexing wiki pages");
			boolean ranked = indexWikiPages(links, pages);
			if (!ranked) {
				log.error("Failed to complete indexing of wiki pages");
				System.exit(2);
			}
		} catch (Exception e) {
			log.error("IndexerTool failed to run successfully", e);
			System.exit(99);
		}

		return 0;
	}

	/**
	 * Initializes and executes a Hadoop MapReduce job to extract all pages and
	 * links from the wikipedia page format.
	 * 
	 * @param input
	 *            HDFS reference to the input file
	 * @param links
	 *            HDFS reference for the links index file destination
	 * 
	 * @return
	 * @throws Exception
	 */
	private boolean indexWikiLinks(String input, String links) throws Exception {

		Job job = Job.getInstance(this.getConf(), "WikiWiki_Index_Links");
		job.setJarByClass(IndexerTool.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(WikipediaInputFormat.class);
		job.setMapperClass(PageLinkMapper.class);
		job.setMapOutputKeyClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(links));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(PageLinkReducer.class);

		return job.waitForCompletion(true);

	}

	/**
	 * Initializes and executes a Hadoop MapReduce job to extract all pages from
	 * the links index file.
	 * 
	 * @param links
	 *            HDFS reference to the links index destination
	 * @param pages
	 *            HDFS reference for the pages index destination
	 * 
	 * @return
	 * @throws Exception
	 */
	private boolean indexWikiPages(String links, String pages) throws Exception {
		Job job = Job.getInstance(this.getConf(), "WikiWiki_Index_Pages");
		job.setJarByClass(IndexerTool.class);

		FileInputFormat.addInputPath(job, new Path(links));
		job.setMapperClass(PageTitleMapper.class);
		job.setMapOutputKeyClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(pages));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(PageTitleReducer.class);

		return job.waitForCompletion(true);
	}

}
