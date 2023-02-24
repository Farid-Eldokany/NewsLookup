package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryToQueryList;

/**
 * Reduces each queries to an instance of QueryToQueryList
 * 
 * @author Farid
 */

public class QueryListUnifier implements ReduceFunction<QueryToQueryList> {

	private static final long serialVersionUID = 646837837121185131L;

	@Override
	public QueryToQueryList call(QueryToQueryList v1, QueryToQueryList v2) {

		List<Query> queryList = new ArrayList<Query>();

		queryList.addAll(v1.getQuerytoQueryList());
		queryList.addAll(v2.getQuerytoQueryList());

		v1.setQuerytoQueryList(queryList);
		return v1;
	}

}
