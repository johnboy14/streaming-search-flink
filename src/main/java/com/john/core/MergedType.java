package com.john.core;

import java.util.Map;

public class MergedType {

	private EventType type;
	private Map<String, String> values;

	public MergedType(EventType type, Map<String, String> values) {
		this.type = type;
		this.values = values;
	}

	public EventType getType() {
		return type;
	}

	public Map<String, String> getValues() {
		return values;
	}
}
