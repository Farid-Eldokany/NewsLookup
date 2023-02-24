package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleContent;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleContentToQueryTermCountArray;

/**
 *
 * Fetching queryTermCountInArticle short array from the ArticleContent's
 * object.
 * 
 * @author Farid
 */
public class ArticleContentToQueryTermCount
		implements FlatMapFunction<ArticleContent, ArticleContentToQueryTermCountArray> {

	private static final long serialVersionUID = -3580267664593811948L;

	@Override
	public Iterator<ArticleContentToQueryTermCountArray> call(ArticleContent article) {
		List<ArticleContentToQueryTermCountArray> res = new ArrayList<ArticleContentToQueryTermCountArray>();
		if (Objects.isNull(article)) {
			return new ArrayList<ArticleContentToQueryTermCountArray>(0).iterator();
		}
		res.add(new ArticleContentToQueryTermCountArray(article.getArticleTermCounts()));
		return res.iterator();
	}

}
