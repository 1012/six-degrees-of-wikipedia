package be.casisto.tinkering.wikipedia.indexer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer implementation for listing all outgoing links on a particular wiki
 * page.
 * 
 * @author jiri
 */
public class PageLinkReducer extends Reducer<Text, Text, Text, Text> {

	/**
	 * Reduces the key=title value=link to a single output pair with key=title
	 * value=comma-delimited list of links.
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String links = "";
		boolean first = true;

		for (Text value : values) {
			if (!first)
				links += ",";

			links += value.toString();
			first = false;
		}

		context.write(key, new Text(links));

	}

}
