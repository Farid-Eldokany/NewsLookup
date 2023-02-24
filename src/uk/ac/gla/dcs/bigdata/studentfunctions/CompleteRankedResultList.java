package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.spark.api.java.function.ReduceFunction;


import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

import uk.ac.gla.dcs.bigdata.studentstructures.RankedResultList;
/**
 * Reduccing the RankedResultList data set into one RankedResultList object.
 * @author Farid 
 */
public class CompleteRankedResultList implements ReduceFunction<RankedResultList> {

	
	private static final long serialVersionUID = 9181913590334073212L;


	

	@Override
	public RankedResultList call(RankedResultList rankedResultList1, RankedResultList rankedResultList2)
			throws Exception {
		if (Objects.isNull(rankedResultList1) &&(Objects.isNull(rankedResultList2))) {
			return null;
		} else if (Objects.isNull(rankedResultList1)) {
			return rankedResultList2;
		} else if (Objects.isNull(rankedResultList2)) {
			return rankedResultList1;
			
		}
		List<RankedResult> result= new ArrayList<RankedResult>();
		result.addAll(rankedResultList1.getRankedResultList());
		result.addAll(rankedResultList2.getRankedResultList());
		rankedResultList1.setRankedResultList(result);
		return rankedResultList1;
	}

}
