package be.casisto.tinkering.wikipedia.indexer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer implementation that reduces all pages and calculate the
 * Page Rank for each of them.
 * 
 * @author jiri
 */
public class PageTitleReducer extends Reducer<Text, Text, Text, Text> {

	private static final float PAGE_RANK_DAMPING = 0.85F;

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		float pageRank = 1.0F;
		for (Text value : values) {
			pageRank += (1.0 / Float.parseFloat(value.toString()));
		}

		pageRank = PAGE_RANK_DAMPING * pageRank + (1 - PAGE_RANK_DAMPING);

		context.write(key, new Text(Float.toString(pageRank)));

	}

}
