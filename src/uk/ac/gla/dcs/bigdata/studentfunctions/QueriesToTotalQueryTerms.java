package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.TotalQueryTermsArray;

/**
 * Map each query to an executor saves an array of string of query terms for
 * each query
 * 
 * @author Yu Kit
 */
public class QueriesToTotalQueryTerms implements FlatMapFunction<Query, TotalQueryTermsArray> {

	private static final long serialVersionUID = 4495999958221421447L;

	@Override
	public Iterator<TotalQueryTermsArray> call(Query v1) throws Exception {
		List<TotalQueryTermsArray> result = new ArrayList<TotalQueryTermsArray>();
		List<String> terms = v1.getQueryTerms();
		String[] temp = { "1" };
		for (String s : terms) {
			temp[0] = s;
			if (!result.contains(temp)) {
				result.add(new TotalQueryTermsArray(temp.clone()));
			}
		}

		return result.iterator();

	}

}
