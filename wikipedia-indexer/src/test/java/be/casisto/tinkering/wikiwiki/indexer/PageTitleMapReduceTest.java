package be.casisto.tinkering.wikiwiki.indexer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import be.casisto.tinkering.wikipedia.indexer.PageTitleMapper;
import be.casisto.tinkering.wikipedia.indexer.PageTitleReducer;

/**
 * Unit test implementation for a complete map-reduce cycle for pages
 * @author jiri
 *
 */
public class PageTitleMapReduceTest {

	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> driver;

	@Before
	public void setup() {
		PageTitleMapper mapper = new PageTitleMapper();
		PageTitleReducer reducer = new PageTitleReducer();
		driver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void test() throws IOException {

		driver.withInput(new LongWritable(1), new Text("Page1\tPage 2,Page 3"));

		driver.withOutput(new Text("Page 2"), new Text("1.4250001"));
		driver.withOutput(new Text("Page 3"), new Text("1.4250001"));

		driver.runTest();

	}

}
