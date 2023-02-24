package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleContent;
/**
 * Compile first 5 contents of subtype paragraph and the title of NewsArticle
 * into a string Processes the compiled string using provided document processor
 * Doesn't return a value if title is null
 * 
 * @author Yu Kit
 */
public class NewsArticleToArticleContent implements FlatMapFunction<NewsArticle, ArticleContent> {

	private static final long serialVersionUID = 4929952718886547475L;
	private transient TextPreProcessor processor;
	LongAccumulator termCountAccumulator;
	LongAccumulator articleCountAccumulator;
	Broadcast<String[]> broadcastQueryTerms;

	/**
	 * Compile first 5 contents of subtype paragraph and the title of NewsArticle
	 * into a string Processes the compiled string using provided document processor
	 * Doesn't return a value if title is null
	 * 
	 * @author Yu Kit
	 */

	public NewsArticleToArticleContent(LongAccumulator termCountAccumulator, LongAccumulator articleCountAccumulator,
			Broadcast<String[]> broadcastQueryTerms) {
		this.termCountAccumulator = termCountAccumulator;
		this.articleCountAccumulator = articleCountAccumulator;
		this.broadcastQueryTerms = broadcastQueryTerms;
	}

	@Override
	public Iterator<ArticleContent> call(NewsArticle value) throws Exception {

		List<ArticleContent> result = new ArrayList<ArticleContent>();
		if (processor == null)
			processor = new TextPreProcessor();

		// compile title and article content
		// empty string means that there either title is null or both title is null and
		// contentItem list associated is null
		String originalContent = compilingArticlesTokens(value);

		// processes the NewsArticle
		List<String> articleTerms = processor.process(originalContent);

		// filters out null titles or empty newsArticle
		if (!originalContent.equals("")) {
			this.articleCountAccumulator.add(1);
			this.termCountAccumulator.add(articleTerms.size());
		} else {
			return new ArrayList<ArticleContent>(0).iterator();
		}

		ArticleContent articleContent = new ArticleContent(value);
		articleContent.setArticle(value);
		articleContent.setArticleTermsCount((short) articleTerms.size());
		short[] queryTermCountInArticle = new short[Array.getLength(broadcastQueryTerms.value())];

		// counts query in newsArticle
		HashMap<String, Short> queryMap = new HashMap<String, Short>();
		for (short i = 0; i < Array.getLength(broadcastQueryTerms.value()); i++) {
			queryMap.put(this.broadcastQueryTerms.value()[i], i);
		}
		for (String word : articleTerms) {
			if (queryMap.containsKey(word)) {
				queryTermCountInArticle[queryMap.get(word)] += 1;
			}
		}
		articleContent.setArticleTermCounts(queryTermCountInArticle);

		result.add(articleContent);
		return result.iterator();
	}

	/**
	 * 
	 * @param news
	 * @return String
	 * @author Yu Kit Compile a string of title and first 5 content of subtype
	 *         paragraphs for easy processing Returns empty string if no title or no
	 *         content since either will lead to 0 DPH or impossible textual
	 *         distance score
	 */
	private String compilingArticlesTokens(NewsArticle news) {
		String finalS = "";
		boolean titleNotNull = true;
		if (news.getTitle() == null) {
			titleNotNull = false;
		}
		finalS += news.getTitle() + " ";
		int paragraphsAdded = 0;
		ListIterator<ContentItem> contents = news.getContents().listIterator();

		// compiles the String
		// checks if title appears as a ContentItem
		// checks if subtype of contentItem is paragraph
		while ((contents.hasNext()) && (paragraphsAdded < 5)) {
			String contentSubtype;
			ContentItem c = contents.next();
			if (Objects.isNull(c)) {
				continue;
			}
			try {

				if (!titleNotNull) {
					if (c.getType().equals("title")) {
						if (!Objects.isNull(c.getContent())) {
							String temp = finalS;
							finalS = c.getContent() + " " + temp;
							titleNotNull = true;
						}

					}
				}
			} catch (NullPointerException e) {
				;
			} finally {
				try {
					contentSubtype = c.getSubtype();
					if (contentSubtype == null) {
						continue;
					} else {
						if (contentSubtype.equals("paragraph")) {
							finalS += c.getContent() + " ";
							paragraphsAdded += 1;
						}
					}

				} catch (NullPointerException e) {
					;
				}
			}
		}

		// returns an empty string is there is no title
		if (titleNotNull) {
			return finalS;
		} else {
			return "";
		}
	}
}
