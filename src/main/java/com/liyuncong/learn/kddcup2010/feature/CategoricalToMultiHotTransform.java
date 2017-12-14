package com.liyuncong.learn.kddcup2010.feature;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.datavec.api.transform.transform.categorical.CategoricalToOneHotTransform;
import org.datavec.api.writable.IntWritable;
import org.datavec.api.writable.Writable;

public class CategoricalToMultiHotTransform extends CategoricalToOneHotTransform{

	private static final long serialVersionUID = 7176741129016999694L;
	
	private String delimiter;

	public CategoricalToMultiHotTransform(String columnName, String delimiter) {
		super(columnName);
		this.delimiter = delimiter;
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
            	Set<Integer> classIdxes = new HashSet<>();
                String[] str = w.toString().split(delimiter);
                for (String string : str) {
                	Integer classIdx = getStatesMap().get(string);
                	classIdxes.add(classIdx);
				}
                
                for (int j = 0; j < n; j++) {
                    if (classIdxes.contains(j))
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
