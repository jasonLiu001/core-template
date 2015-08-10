package com.isoftstone.crawl.template.impl;

public class BaseSelector {

	private String type = "";
	private String value = "";

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public BaseSelector() {

	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(super.toString() + "\n");
		if (this.type != null)
			str.append("TYPE: " + this.type + "\n");
		if (this.value != null)
			str.append("VALUE: " + value + "\n");
		return str.toString();
	}
}
