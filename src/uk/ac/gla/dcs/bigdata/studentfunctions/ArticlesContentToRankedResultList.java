package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleContent;
import uk.ac.gla.dcs.bigdata.studentstructures.RankedResultList;

/**
 * Calculates the average DPH score of each query terms related to a query
 * 
 * @author Yu Kit
 */
public class ArticlesContentToRankedResultList implements FlatMapFunction<ArticleContent, RankedResultList> {

	private static final long serialVersionUID = -4976181592937052812L;

	// current query being iterated over
	Broadcast<List<Query>> broadcastQuery;
	// query terms in corpus
	Broadcast<short[]> broadcastQueryArticleTermCountsArrray;
	// total terms in corpus (sum of all terms of documents in the corpus)
	Broadcast<Long> broadcastTermCountAccumulator;
	// total number of document in corpus
	Broadcast<Long> broadcastArticleCountAccumulator;
	// array of all query terms corresponds to index of
	// broadcastQueryArticleTermCountsArrray
	Broadcast<String[]> broadcastQueryTerms;

	int queryIndex;
	/**
	 * Calculates the average DPH score of each query terms related to a query
	 * 
	 * @author Yu Kit
	 */
	public ArticlesContentToRankedResultList(int queryIndex, Broadcast<String[]> broadcastQueryTerms,
			Broadcast<List<Query>> broadcastQuery, Broadcast<short[]> broadcastQueryArticleTermCountsArrray,
			Broadcast<Long> broadcastTermCountAccumulator, Broadcast<Long> broadcastArticleCountAccumulator) {

		this.queryIndex = queryIndex;
		this.broadcastQueryTerms = broadcastQueryTerms;
		this.broadcastQuery = broadcastQuery;
		this.broadcastQueryArticleTermCountsArrray = broadcastQueryArticleTermCountsArrray;
		this.broadcastTermCountAccumulator = broadcastTermCountAccumulator;
		this.broadcastArticleCountAccumulator = broadcastArticleCountAccumulator;
	}

	@Override
	public Iterator<RankedResultList> call(ArticleContent value) throws Exception {
		if (Objects.isNull(value)) {
			return null;
		}
		RankedResultList result = new RankedResultList();
		List<RankedResultList> resultList = new ArrayList<RankedResultList>(0);

		long totalDocsInCorpus = this.broadcastArticleCountAccumulator.value();
		long totalTermsInCorpus = this.broadcastTermCountAccumulator.value();

		// calculate average number of terms in each article
		double averageDocumentLengthInCorpus = ((double) totalTermsInCorpus) / ((double) totalDocsInCorpus);

		// make HashMap of each query terms to previously accumulated terms for O(1)
		// access
		HashMap<String, Short> corpusQueryMap = new HashMap<String, Short>();
		for (int i = 0; i < Array.getLength(this.broadcastQueryTerms.value()); i++) {
			corpusQueryMap.put(this.broadcastQueryTerms.value()[i],
					this.broadcastQueryArticleTermCountsArrray.value()[i]);
		}
		HashMap<String, Short> documentQueryMap = new HashMap<String, Short>();
		short[] articleTermCount = value.getArticleTermCounts();
		for (int i = 0; i < Array.getLength(this.broadcastQueryTerms.value()); i++) {
			documentQueryMap.put(this.broadcastQueryTerms.value()[i], articleTermCount[i]);
		}

		// define variable to each DPH for each query and count
		double totalDPH = 0;

		// iterate through each query term
		Query currentQuery = this.broadcastQuery.value().get(this.queryIndex);
		List<String> doneQueries = new ArrayList<String>();
		int count = currentQuery.getQueryTerms().size();
		for (String term : currentQuery.getQueryTerms()) {
			if (!doneQueries.contains(term)) {
				short queryInDocument = documentQueryMap.get(term);
				int queryInCorpus = corpusQueryMap.get(term);
				int documentLength = value.getArticleTermsCount();
				if ((documentLength != 0)) {
					if (queryInDocument > 0) {
						double t = DPHScorer.getDPHScore(queryInDocument, queryInCorpus, documentLength,
								averageDocumentLengthInCorpus, totalDocsInCorpus);
						totalDPH += t;
					}
				}
				doneQueries.add(term);
			}
		}

		// add DPH for each document to each query
		// calculate average DPH
		double score = totalDPH / count;
		if (score == 0) {
			return new ArrayList<RankedResultList>(0).iterator();
		}

		// make rankedResult to be added to result
		RankedResult rankedResult = new RankedResult();
		rankedResult.setScore(score);
		rankedResult.setArticle(value.getArticle());
		rankedResult.setDocid(value.getArticle().getId());

		result.add(rankedResult);
		resultList.add(result);
		return resultList.iterator();
	}

}
