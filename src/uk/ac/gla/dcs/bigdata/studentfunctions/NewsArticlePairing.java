package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleContent;

/**
 * 
 * converts RDD of article content to a Tuple2<NewsArticle, ArticleContent>
 * where the News article is a class to implement key value pairing for article
 * content.
 * 
 * @author Morgan
 */
public class NewsArticlePairing implements PairFunction<ArticleContent, NewsArticle, ArticleContent> {

	private static final long serialVersionUID = -5195778779217447485L;

	@Override
	public Tuple2<NewsArticle, ArticleContent> call(ArticleContent value) throws Exception {
		Tuple2<NewsArticle, ArticleContent> pairing = new Tuple2<NewsArticle, ArticleContent>(value.getArticle(),
				value);
		return pairing;
	}

}
