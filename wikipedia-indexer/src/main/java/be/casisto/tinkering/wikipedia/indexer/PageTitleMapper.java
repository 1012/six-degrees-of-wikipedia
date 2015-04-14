package be.casisto.tinkering.wikipedia.indexer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper implementation to extract all pages and the number of links to them.
 * 
 * @author jiri
 */
public class PageTitleMapper extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * Mapper implementation that maps source / target links pair to target link
	 * / number of links in source.
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String raw = Text.decode(value.getBytes());
		String linkList = raw.split("\t")[1];

		String[] links = linkList.split(",");
		long numLinks = links.length;

		for (String link : links) {
			context.write(new Text(link), new Text(Long.toString(numLinks)));
		}
	}

}
