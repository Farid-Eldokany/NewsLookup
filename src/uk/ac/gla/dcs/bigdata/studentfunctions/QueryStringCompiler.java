package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.lang.reflect.Array;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.TotalQueryTermsArray;

/**
 * Compiles the array of string of query terms into a single array
 * 
 * @author Yu Kit
 */

public class QueryStringCompiler implements ReduceFunction<TotalQueryTermsArray> {

	private static final long serialVersionUID = 3098562654198002741L;

	@Override
	public TotalQueryTermsArray call(TotalQueryTermsArray v1, TotalQueryTermsArray v2) {
		// TODO Auto-generated method stub
		int len1 = Array.getLength(v1.getTotalQueryTerms());
		int len2 = Array.getLength(v2.getTotalQueryTerms());
		String[] result = new String[len1 + len2];
		System.arraycopy(v1.getTotalQueryTerms(), 0, result, 0, len1);
		System.arraycopy(v2.getTotalQueryTerms(), 0, result, len1, len2);
		v1.setTotalQueryTerms(result);
		return v1;
	}

}
