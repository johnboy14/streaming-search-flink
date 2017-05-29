package com.john.core;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import uk.co.flax.luwak.QueryMatch;

import static java.text.MessageFormat.format;

public class MatchedEventSink implements SinkFunction<QueryMatch> {

	@Override
	public void invoke(QueryMatch queryMatch) throws Exception {
		System.out.println(format("MDEventId={0}, QueryId={1}", queryMatch.getDocId(), queryMatch.getQueryId()));
	}

}
