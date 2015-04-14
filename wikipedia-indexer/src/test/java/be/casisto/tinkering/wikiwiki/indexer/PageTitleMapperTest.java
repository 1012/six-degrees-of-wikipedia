package be.casisto.tinkering.wikiwiki.indexer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import be.casisto.tinkering.wikipedia.indexer.PageTitleMapper;

/**
 * Unit test implementation for the PageTitleMapper.
 * 
 * @author jiri
 */
public class PageTitleMapperTest {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;

	@Before
	public void setup() {
		PageTitleMapper mapper = new PageTitleMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	public void test() throws IOException {

		mapDriver.withInput(new LongWritable(1), new Text(
				"Page 1\tPage 2,Page 3"));

		mapDriver.withOutput(new Text("Page 2"), new Text("2"));
		mapDriver.withOutput(new Text("Page 3"), new Text("2"));

		mapDriver.runTest();

	}

}
