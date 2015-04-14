package be.casisto.tinkering.wikiwiki.indexer;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import be.casisto.tinkering.wikipedia.indexer.PageLinkMapper;
import be.casisto.tinkering.wikipedia.indexer.PageLinkReducer;
import be.casisto.tinkering.wikiwiki.indexer.TestHelper;

/**
 * Unit test implementation for a complete map-reduce cycle.
 * 
 * @author jiri
 */
public class PageLinkMapReduceTest {

	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> driver;

	@Before
	public void setup() {
		PageLinkMapper mapper = new PageLinkMapper();
		PageLinkReducer reducer = new PageLinkReducer();
		driver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void test() throws IOException {

		String content = null;
		try {
			content = TestHelper.readFile("src/test/resources/single-page.txt");
		} catch (IOException e) {
			fail(e.getMessage());
		}

		LongWritable key = new LongWritable(1);
		Text val = new Text(content);

		driver.withInput(key, val);
		driver.withOutput(new Text("Page 1"), new Text("Page 2,Page 2,Page 3"));

		driver.runTest();

	}

}
