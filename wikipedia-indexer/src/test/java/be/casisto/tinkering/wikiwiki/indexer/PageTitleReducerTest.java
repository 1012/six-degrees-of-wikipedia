package be.casisto.tinkering.wikiwiki.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import be.casisto.tinkering.wikipedia.indexer.PageTitleReducer;

public class PageTitleReducerTest {

	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setup() {
		PageTitleReducer reducer = new PageTitleReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void test() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("2"));

		reduceDriver.withInput(new Text("Page 2"), values);
		reduceDriver.withInput(new Text("Page 3"), values);

		reduceDriver.withOutput(new Text("Page 2"), new Text("1.4250001"));
		reduceDriver.withOutput(new Text("Page 3"), new Text("1.4250001"));

		reduceDriver.runTest();
	}

}
