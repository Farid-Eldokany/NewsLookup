package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

/**
 * Represents the RankedResults objects in a List.
 * 
 * @author farid
 */
public class RankedResultList implements Serializable {

	private static final long serialVersionUID = -6128082381682056804L;
	List<RankedResult> rankedResultList;

	public RankedResultList() {
		this.rankedResultList = new ArrayList<RankedResult>();
	}

	public void add(RankedResult value) {
		this.rankedResultList.add(value);
	}

	public List<RankedResult> getRankedResultList() {
		return rankedResultList;
	}

	public void setRankedResultList(List<RankedResult> rankedResultList) {

		this.rankedResultList = rankedResultList;
	}

}
