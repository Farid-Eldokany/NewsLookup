package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryToQueryList;

/**
 * Maps a query to each executor to be reduced to a list
 * 
 * @author Farid
 */

public class queryToQueryLists implements MapFunction<Query, QueryToQueryList> {

	private static final long serialVersionUID = 8300061331863361961L;

	@Override
	public QueryToQueryList call(Query v1) {

		return new QueryToQueryList(v1);
	}

}
