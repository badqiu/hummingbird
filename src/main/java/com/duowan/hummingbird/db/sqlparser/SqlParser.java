package com.duowan.hummingbird.db.sqlparser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.apache.commons.lang.StringUtils;

import com.duowan.hummingbird.db.sql.select.FromItem;
import com.duowan.hummingbird.db.sql.select.OrderBy;
import com.duowan.hummingbird.db.sql.select.SelectSql;
import com.github.rapid.common.util.RegexUtil;

public class SqlParser {

	
	public SelectSql parseForSelectSql(String query) throws JSQLParserException {
		if(StringUtils.isBlank(query)) {
			return null;
		}
		CCJSqlParserManager pm = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement stmt = pm.parse(new StringReader(query));
		if (stmt instanceof Select) {
			Select s = (Select)stmt;
			SelectBody selectBody = s.getSelectBody();
			final SelectSql result = parseSelectBody(selectBody);
			
//			if(StringUtils.isEmpty(result.getInto())) {
//				String into = RegexUtil.findByRegexGroup(query, "(?i)into\\s+([\\w_]+)", 1);
//				result.setInto(into);
//			}
			return result;
		}else {
			throw new RuntimeException("only parse select sql,current sql:"+query);
		}
	}

	private SelectSql parseSelectBody(SelectBody selectBody) {
		final SelectSql result = new SelectSql();
		selectBody.accept(new BaseSelectVisitor() {
			@Override
			public void visit(PlainSelect plainSelect) {
//					System.out.println("select:"+plainSelect.getSelectItems().toString());
//					System.out.println("from:"+fromItem );
//					System.out.println("where:"+plainSelect.getWhere());
//					System.out.println("groupBy:"+plainSelect.getGroupByColumnReferences().toString());
				
				setJoins(result,plainSelect.getJoins());
				final List<com.duowan.hummingbird.db.sql.select.SelectItem> selectItems = getSelectItems(plainSelect);
				result.setSelectItems(selectItems.toArray(new com.duowan.hummingbird.db.sql.select.SelectItem[0]));
				result.setWhere(plainSelect.getWhere() == null ? null : plainSelect.getWhere().toString());
				result.setFrom(newFromItem(plainSelect.getFromItem()));
				result.setOrderBy(newOrderBy(plainSelect.getOrderByElements()));
				setGroupBy(result, plainSelect);
				setLimit(result, plainSelect);
				result.setHaving(plainSelect.getHaving() == null ? null : plainSelect.getHaving().toString());
				
//				Expression expr = plainSelect.getWhere();
				String into = toInto(plainSelect.getIntoTables());
				result.setInto(into);
			}



			private String toInto(List<Table> intoTables) {
				if(intoTables == null) return null;
				
				List<String> result = new ArrayList<String>();
				for(Table t : intoTables) {
					result.add(t.getName());
				}
				return StringUtils.join(result,",");
			}



			private List<com.duowan.hummingbird.db.sql.select.SelectItem> getSelectItems(
					PlainSelect plainSelect) {
				final List<com.duowan.hummingbird.db.sql.select.SelectItem> selectItems = new ArrayList<com.duowan.hummingbird.db.sql.select.SelectItem>();
				for(SelectItem item : plainSelect.getSelectItems()) {
					item.accept(new BaseSelectItemVisitor() {
						@Override
						public void visit(SelectExpressionItem selectExpressionItem) {
							final com.duowan.hummingbird.db.sql.select.SelectItem selectItem = new com.duowan.hummingbird.db.sql.select.SelectItem();
							selectExpressionItem.getExpression().accept(new BaseExpressionVisitor(){
								@Override
								public void visit(Function function) {
//										System.out.println("func: "+function.getName()  + " params:"+ function.getParameters().getExpressions());
									selectItem.setFunc(function.getName());
									List<String> params = new ArrayList();
									for(Expression paramExpr : function.getParameters().getExpressions()) {
										params.add(paramExpr.toString());
									}
									selectItem.setParams(params.toArray(new String[0]));
								}
							});
							selectItem.setExpr(selectExpressionItem.getExpression().toString());
							selectItem.setAlias(selectExpressionItem.getAlias() == null ? null : selectExpressionItem.getAlias().getName());
							selectItems.add(selectItem);
//								System.out.println("expr: " +selectExpressionItem.getExpression() + " as "+ selectExpressionItem.getAlias());
						}
						
						@Override
						public void visit(AllColumns allColumns) {
							com.duowan.hummingbird.db.sql.select.SelectItem selectItem = new com.duowan.hummingbird.db.sql.select.SelectItem();
							selectItem.setAllTableColumns(true);
							selectItems.add(selectItem);
						}
					});
				}
				return selectItems;
			}



			private void setLimit(final SelectSql result,
					PlainSelect plainSelect) {
				if(plainSelect.getLimit() != null) {
					result.setLimit(plainSelect.getLimit().getRowCount());
					result.setOffset(plainSelect.getLimit().getOffset());
				}
			}



			private void setGroupBy(final SelectSql result,
					PlainSelect plainSelect) {
				if(plainSelect.getGroupByColumnReferences() != null) {
					List<String> groupByList = new ArrayList();
					for(Expression expr : plainSelect.getGroupByColumnReferences() ){
						groupByList.add(expr.toString());
					}
					result.setGroupBy(StringUtils.join(groupByList,","));
				}
			}



			private OrderBy[] newOrderBy( List<OrderByElement> orderByElements) {
				if(orderByElements == null) return null;
				
				List<OrderBy> result = new ArrayList<OrderBy>();
				for(OrderByElement item : orderByElements) {
					result.add(new OrderBy(item.getExpression().toString(),item.isAsc()));
				}
				return result.toArray(new OrderBy[result.size()]);
			}


			private void setJoins(SelectSql result, List<Join> joins) {
				if(joins == null) return;
				List<com.duowan.hummingbird.db.sql.select.Join> joinList = new ArrayList<com.duowan.hummingbird.db.sql.select.Join>();
				for(Join join : joins) {
					com.duowan.hummingbird.db.sql.select.Join temp = new com.duowan.hummingbird.db.sql.select.Join();
					temp.setOn(join.getOnExpression().toString());
					temp.setRightItem(newFromItem(join.getRightItem()));
					joinList.add(temp);
				}
				result.setJoins(joinList.toArray(new com.duowan.hummingbird.db.sql.select.Join[0]));
			}
		});
		return result;
	}
	
	private FromItem newFromItem(net.sf.jsqlparser.statement.select.FromItem fromItem) {
		if(fromItem instanceof net.sf.jsqlparser.schema.Table) {
			net.sf.jsqlparser.schema.Table table = (net.sf.jsqlparser.schema.Table)fromItem;
			return new FromItem(table.getName(),table.getAlias() == null ? null : table.getAlias().toString());
		}else if(fromItem instanceof SubSelect) {
			SubSelect subSelect = (SubSelect)fromItem;
			SelectSql subSelectSql = parseSelectBody(subSelect.getSelectBody());
			return new FromItem(subSelectSql,fromItem.getAlias() == null ? null : fromItem.getAlias().toString());
		}else {
			throw new RuntimeException("unknow fromItem:"+fromItem);
		}
	}
	
}
