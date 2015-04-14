package be.casisto.tinkering.wikipedia.indexer;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper implementation to extract and map all links in a wikipedia page.
 * 
 * @author jiri
 */
public class PageLinkMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final Logger log = LoggerFactory
			.getLogger(PageLinkMapper.class);

	private static final Pattern linkPattern = Pattern.compile("\\[.+?\\]");

	private static final String TITLE_START_TAG = "<title>";
	private static final String TITLE_END_TAG = "</title>";

	/**
	 * Parse the wiki page and extract links. Mapper extracts links to pages and
	 * outputs key=<title> value=<comma-delimited links> pairs.
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		// extract the page title and content
		String[] titleAndText = parseTitleAndText(value);

		if (!isValidPage(titleAndText[0]))
			return;

		log.debug("Extracted wiki page with title {}", titleAndText[0]);

		// prepare the Text key
		Text page = new Text(titleAndText[0]);

		// extract the links in the page content
		Matcher matcher = linkPattern.matcher(titleAndText[1]);
		long count = 0;
		while (matcher.find()) {
			String link = matcher.group();

			// Filter non-wiki links out
			link = parseWikiLink(link);
			if (link != null) {
				log.debug("Added link {} to page {}.", ++count, link);
				context.write(page, new Text(link));
			}
		}
	}

	/**
	 * Extracts the title and page content from the wikipedia page.
	 * 
	 * @param value
	 *            raw content of the wiki page
	 * @return
	 * @throws CharacterCodingException
	 */
	private String[] parseTitleAndText(Text value)
			throws CharacterCodingException {
		String[] titleAndText = new String[2];

		int start = value.find(TITLE_START_TAG);
		int end = value.find(TITLE_END_TAG);

		start += 7; // add the length of the start tag itself;

		// extract the title from the page xml
		titleAndText[0] = Text.decode(value.getBytes(), start, end - start);

		start = value.find("<text");
		start = value.find(">", start); // text tag contains additional info:
										// <text xml:space="preserve"
										// bytes="17745">
		end = value.find("</text>", start);
		start += 1;

		if (start == -1 || end == -1) {
			return new String[] { "", "" };
		}

		titleAndText[1] = Text.decode(value.getBytes(), start, end - start);

		return titleAndText;
	}

	/**
	 * Parse wiki links to extract the wiki page from the link.
	 * 
	 * @param link
	 * @return
	 */
	private String parseWikiLink(String link) {

		if (!isWikiLink(link))
			return null;

		int start = link.startsWith("[[") ? 2 : 1;
		int end = link.indexOf("]");

		int pipe = link.indexOf("|");
		if (pipe > 0)
			end = pipe;

		int part = link.indexOf("#");
		if (part > 0)
			end = part;

		link = link.substring(start, end);
		link.replaceAll("\\s", " ");
		link.replaceAll(",", "");
		link.replaceAll("&amp", "&");

		return link;
	}

	/**
	 * Check is the link is a page vs category.
	 * 
	 * @param title
	 * @return
	 */
	public boolean isValidPage(String title) {
		return !title.contains(":");
	}

	/**
	 * Check if the link is a valid link to a wiki page.
	 * 
	 * @param link
	 * @return
	 */
	private boolean isWikiLink(String link) {
		int start = 1;

		if (link.startsWith("[["))
			start = 2;

		if (link.length() < start + 2 || link.length() > 100)
			return false;

		char firstChar = link.charAt(start);

		if (firstChar == '#')
			return false;
		if (firstChar == ',')
			return false;
		if (firstChar == '.')
			return false;
		if (firstChar == '&')
			return false;
		if (firstChar == '\'')
			return false;
		if (firstChar == '-')
			return false;
		if (firstChar == '{')
			return false;

		if (link.contains(":"))
			return false; // Matches: external links and translations links
		if (link.contains(","))
			return false; // Matches: external links and translations links
		if (link.contains("&"))
			return false;

		return true;
	}

}
