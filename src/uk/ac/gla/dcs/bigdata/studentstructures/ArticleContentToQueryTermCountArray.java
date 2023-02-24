package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

/**
 * Represents the query terms count across NewArticle content in a short array.
 * 
 * @author farid
 */
public class ArticleContentToQueryTermCountArray implements Serializable {

	private static final long serialVersionUID = 4723817370367951405L;
	short[] articleContentToQueryTermCount;

	public ArticleContentToQueryTermCountArray(short[] articleContentToQueryTermCount) {
		this.articleContentToQueryTermCount = articleContentToQueryTermCount;
	}

	public short[] getArticleContentToQueryTermCount() {
		return articleContentToQueryTermCount;
	}

	public void setArticleContentToQueryTermCount(short[] articleContentToQueryTermCount) {
		this.articleContentToQueryTermCount = articleContentToQueryTermCount;
	}

}
