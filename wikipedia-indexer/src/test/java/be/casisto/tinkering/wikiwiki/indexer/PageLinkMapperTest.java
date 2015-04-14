package be.casisto.tinkering.wikiwiki.indexer;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import be.casisto.tinkering.wikipedia.indexer.PageLinkMapper;
import be.casisto.tinkering.wikiwiki.indexer.TestHelper;

/**
 * Unit test class for PageLinkMapper.
 * 
 * @author jiri
 */
public class PageLinkMapperTest {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;

	@Before
	public void setup() {
		PageLinkMapper mapper = new PageLinkMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
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

		mapDriver.withInput(key, val);
		mapDriver.withOutput(new Text("Page 1"), new Text("Page 2"));
		mapDriver.withOutput(new Text("Page 1"), new Text("Page 2"));
		mapDriver.withOutput(new Text("Page 1"), new Text("Page 3"));

		mapDriver.runTest();
	}

}
