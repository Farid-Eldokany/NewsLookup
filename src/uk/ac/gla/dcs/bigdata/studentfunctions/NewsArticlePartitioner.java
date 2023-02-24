package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.Partitioner;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
/**
 * 
 * Custom partitioning function for use on strings.Extracts the article and
 * unique article identifier to generate a key using the identifier. uses the
 * key to generate a partition number.
 * 
 * @author Morgan
 */
public class NewsArticlePartitioner extends Partitioner {

	private static final long serialVersionUID = 1088171909782712006L;
	final int numberOfPartitions;

	/**
	 * 
	 * Custom partitioning function for use on strings.Extracts the article and
	 * unique article identifier to generate a key using the identifier. uses the
	 * key to generate a partition number.
	 * 
	 * @author Morgan
	 */
	public NewsArticlePartitioner(int numberOfPartions) {
		this.numberOfPartitions = numberOfPartions;
	}

	@Override
	public int getPartition(Object key) {
		NewsArticle item = (NewsArticle) key;
		int partition = item.getId().charAt(0) % numberOfPartitions;
		return partition;
	}

	@Override
	public int numPartitions() {
		return numberOfPartitions;
	}

}
