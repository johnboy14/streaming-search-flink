package com.john.operator;


import com.john.core.EventType;
import com.john.core.MergedType;
import com.john.core.MDEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.Map;

public class MDEventMapFunction implements MapFunction<MDEvent, MergedType> {

	@Override
	public MergedType map(MDEvent mdEvent) throws Exception {
		Map<String, String> map = new HashMap<>();
		map.put("volume", mdEvent.getVolume());
		return new MergedType(EventType.MDEVENT, map);
	}

}
