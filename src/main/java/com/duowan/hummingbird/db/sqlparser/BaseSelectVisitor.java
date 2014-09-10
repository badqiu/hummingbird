package com.duowan.hummingbird.db.sqlparser;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;

public class BaseSelectVisitor implements SelectVisitor {

	@Override
	public void visit(PlainSelect plainSelect) {
	}

	@Override
	public void visit(SetOperationList setOpList) {
	}

	@Override
	public void visit(WithItem withItem) {
	}

}
