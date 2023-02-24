package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * 
 * Represents the analaysis of a single NewsArticle object.
 * 
 * @author Farid
 * 
 */
public class ArticleContent implements Serializable {

	private static final long serialVersionUID = 339203041311360182L;
	NewsArticle article;
	short[] queryTermCountInArticle;
	short articleTermsCount;

	public short getArticleTermsCount() {
		return articleTermsCount;
	}

	public void setArticleTermsCount(short articleTermsCount) {
		this.articleTermsCount = articleTermsCount;
	}

	public ArticleContent(NewsArticle article) {
		this.article = article;
	}

	public NewsArticle getArticle() {
		return this.article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public short[] getArticleTermCounts() {
		return this.queryTermCountInArticle;
	}

	public void setArticleTermCounts(short[] articleTermCounts) {
		this.queryTermCountInArticle = articleTermCounts;
	}

}
