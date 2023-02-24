package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

/**
 * Represents the query terms in a List.
 * 
 * @author farid
 */
public class TotalQueryTermsArray implements Serializable {

	private static final long serialVersionUID = -1257038402761345369L;
	String[] totalQueryTerms;

	public TotalQueryTermsArray(String[] totalQueryTerms) {
		this.totalQueryTerms = totalQueryTerms;
	}

	public String[] getTotalQueryTerms() {
		return totalQueryTerms;
	}

	public void setTotalQueryTerms(String[] totalQueryTerms) {
		this.totalQueryTerms = totalQueryTerms;
	}

}
