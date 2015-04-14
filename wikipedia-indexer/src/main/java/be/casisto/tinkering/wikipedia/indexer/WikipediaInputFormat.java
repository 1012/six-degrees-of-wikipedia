package be.casisto.tinkering.wikipedia.indexer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides an input format implementation specific to wikipedia's
 * xml format.
 * 
 * @author jiri
 */
public class WikipediaInputFormat extends TextInputFormat {

	private static final Logger log = LoggerFactory
			.getLogger(WikipediaInputFormat.class);

	private static final String PAGE_START_TAG = "<page>";
	private static final String PAGE_END_TAG = "</page>";

	/**
	 * Initializes a new record reader.
	 */
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {

		try {
			log.debug("Initializing wiki xml reader");
			return new WikiXmlReader((FileSplit) split, context);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * This class parses wikipedia xml files to search for page content.
	 * 
	 * @author jiri
	 */
	public static class WikiXmlReader extends RecordReader<LongWritable, Text> {

		private final long start;
		private final long end;
		private final FSDataInputStream fsdin;
		private final DataOutputBuffer dob = new DataOutputBuffer();

		private LongWritable key = new LongWritable();
		private Text value = new Text();

		/**
		 * Constructor that initializes the parser for a wikipedia page xml
		 * file.
		 * 
		 * @param split
		 *            The wikipedia page xml
		 * @param context
		 *            The hadoop job context
		 * @throws IOException
		 */
		public WikiXmlReader(FileSplit split, TaskAttemptContext context)
				throws IOException {
			Configuration conf = context.getConfiguration();

			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(conf);

			fsdin = fs.open(file);

			start = split.getStart();
			end = start + split.getLength();

			fsdin.seek(start);
		}

		/**
		 * Initializes the record reader.
		 */
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
		}

		/**
		 * Finds and extracts the wiki page content from the page xml.
		 */
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			log.debug("Looking for next wiki page start tag");
			if (fsdin.getPos() < end) {
				if (readUntilMatch(PAGE_START_TAG.getBytes(), false)) {
					try {
						log.debug("Wiki page content found");
						dob.write(PAGE_START_TAG.getBytes());
						if (readUntilMatch(PAGE_END_TAG.getBytes(), true)) {
							key.set(fsdin.getPos());
							value.set(dob.getData(), 0, dob.getLength());
							return true;
						}
					} finally {
						dob.reset();
					}
				}
			}

			log.debug("No page start tag found");
			return false;
		}

		/**
		 * Returns the current key.
		 */
		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		/**
		 * Returns the current page content as a value.
		 */
		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public void close() throws IOException {
			fsdin.close();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (fsdin.getPos() - start) / (float) (end - start);
		}

		/**
		 * Reads through the input file until a match is found or until the end.
		 * 
		 * @param match
		 *            String to match. Page start or end tag.
		 * @param inPage
		 *            Copy the content to the output buffer if true
		 * @return
		 * @throws IOException
		 */
		private boolean readUntilMatch(byte[] match, boolean inPage)
				throws IOException {
			int i = 0;
			while (true) {
				int b = fsdin.read();

				// end of file
				if (b == -1)
					return false;

				// copy content to buffer
				if (inPage)
					dob.write(b);

				// check for match
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;

				// check if we're past the end point
				if (!inPage && i == 0 && fsdin.getPos() >= end)
					return false;
			}
		}

	}

}
