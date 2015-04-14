package be.casisto.tinkering.wikiwiki.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import be.casisto.tinkering.wikipedia.indexer.PageLinkReducer;

/**
 * Unit test class for the PageLinkReducer implementation.
 * 
 * @author jiri
 */
public class PageLinkReducerTest {

	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setup() {
		PageLinkReducer reducer = new PageLinkReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void test() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("Page 2"));
		values.add(new Text("Page 2"));
		values.add(new Text("Page 3"));

		reduceDriver.withInput(new Text("Page 1"), values);
		reduceDriver.withOutput(new Text("Page 1"), new Text(
				"Page 2,Page 2,Page 3"));

		reduceDriver.runTest();
	}

}
