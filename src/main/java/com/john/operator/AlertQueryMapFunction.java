package com.john.operator;

import com.john.core.EventType;
import com.john.core.MergedType;
import com.john.core.AlertQuery;
import org.apache.flink.api.common.functions.MapFunction;

public class AlertQueryMapFunction implements MapFunction<AlertQuery, MergedType> {

	@Override
	public MergedType map(AlertQuery alertQuery) throws Exception {
		return new MergedType(EventType.QUERY, alertQuery.getQueryMap());
	}

}
