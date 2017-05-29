package com.john.operator;

import com.john.core.EventType;
import com.john.core.MergedType;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import java.util.UUID;

public class LuwakSearchMapFunction extends RichFlatMapFunction<MergedType, QueryMatch> {

	private transient Monitor monitor;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		monitor = new Monitor(new LuceneQueryParser("text", new StandardAnalyzer()), new TermFilteredPresearcher());
	}

	@Override
	public void flatMap(MergedType mergedType, Collector<QueryMatch> collector) throws Exception {

		//If query, then add to index
		if (mergedType.getType().equals(EventType.QUERY)) {
			MonitorQuery query = makeQuery(mergedType);
			monitor.update(query);
			System.out.println(monitor.getQueryCount());
		} else {
			InputDocument doc = InputDocument.builder(UUID.randomUUID().toString())
					.addField("volume", mergedType.getValues().get("volume"), new StandardAnalyzer())
					.build();
			Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
			matches.iterator().forEachRemaining((queryMatches -> queryMatches.iterator().forEachRemaining(collector::collect)));
		}

	}

	private MonitorQuery makeQuery(MergedType mergedType) {
		String volume = mergedType.getValues().get("volume");
		return new MonitorQuery(UUID.randomUUID().toString(), "volume:" + volume);
	}
}
