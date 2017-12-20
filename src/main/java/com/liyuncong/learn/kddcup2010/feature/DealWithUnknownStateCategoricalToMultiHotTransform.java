package com.liyuncong.learn.kddcup2010.feature;

import java.util.ArrayList;
import java.util.List;

import org.datavec.api.transform.transform.categorical.CategoricalToOneHotTransform;
import org.datavec.api.writable.IntWritable;
import org.datavec.api.writable.Writable;
import org.nd4j.shade.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class DealWithUnknownStateCategoricalToMultiHotTransform extends CategoricalToOneHotTransform{

	private static final long serialVersionUID = 4029211324862642655L;

	public DealWithUnknownStateCategoricalToMultiHotTransform(@JsonProperty("columnName") String columnName) {
		super(columnName);
	}
	
	  @Override
	    public List<Writable> map(List<Writable> writables) {
	        if (writables.size() != inputSchema.numColumns()) {
	            throw new IllegalStateException("Cannot execute transform: input writables list length (" + writables.size()
	                            + ") does not " + "match expected number of elements (schema: " + inputSchema.numColumns()
	                            + "). Transform = " + toString());
	        }
	        int idx = getColumnIdx();

	        int n = getStateNames().size();
	        List<Writable> out = new ArrayList<>(writables.size() + n);

	        int i = 0;
	        for (Writable w : writables) {

	            if (i++ == idx) {
	                //Do conversion
	                String str = w.toString();
	                Integer classIdx = getStatesMap().get(str);
	                for (int j = 0; j < n; j++) {
	                    if (classIdx != null && j == classIdx)
	                        out.add(new IntWritable(1));
	                    else
	                        out.add(new IntWritable(0));
	                }
	            } else {
	                //No change to this column
	                out.add(w);
	            }
	        }
	        return out;
	    }
}
