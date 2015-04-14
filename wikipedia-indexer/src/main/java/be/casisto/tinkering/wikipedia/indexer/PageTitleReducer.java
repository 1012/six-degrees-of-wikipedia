package be.casisto.tinkering.wikipedia.indexer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer implementation that reduces the keys to a single entry for each page.
 * 
 * @author jiri
 */
public class PageTitleReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		context.write(key, new Text(""));

	}

}
