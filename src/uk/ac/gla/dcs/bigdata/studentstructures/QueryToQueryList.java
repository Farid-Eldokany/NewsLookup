package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 ** Represents the Query objects in a List.
 * 
 * @author Farid
 */
public class QueryToQueryList implements Serializable {

	private static final long serialVersionUID = -5085181125886203L;
	List<Query> querytoQueryList = new ArrayList<Query>();

	public QueryToQueryList(Query query) {
		this.querytoQueryList.add(query);
	}

	public List<Query> getQuerytoQueryList() {
		return querytoQueryList;
	}

	public void setQuerytoQueryList(List<Query> querytoQueryList) {
		this.querytoQueryList = querytoQueryList;
	}

}
