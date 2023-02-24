package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Objects;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.ArticleContentToQueryTermCountArray;

/**
 * Reducing the ArticleContentToQueryTermCountArray dataset into one
 * articleContentToQueryTermCountArray short array.
 * 
 * @author Farid
 */
public class SumQueryTermCounts implements ReduceFunction<ArticleContentToQueryTermCountArray> {

	private static final long serialVersionUID = -2254338620633643231L;

	@Override
	public ArticleContentToQueryTermCountArray call(ArticleContentToQueryTermCountArray v1,
			ArticleContentToQueryTermCountArray v2) {
		if (Objects.isNull(v1)) {
			return v2;
		} else if (Objects.isNull(v2)) {
			return v1;
		}
		short[] queryTermCounts1 = v1.getArticleContentToQueryTermCount();
		short[] queryTermCounts2 = v2.getArticleContentToQueryTermCount();
		for (int i = 0; i < queryTermCounts1.length; i++)
			queryTermCounts1[i] += queryTermCounts2[i];
		v1.setArticleContentToQueryTermCount(queryTermCounts1);
		return v1;
	}

}
