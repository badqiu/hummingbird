package com.duowan.hummingbird.util;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public class SpringJdbcUtil {

	public static int[] batchUpdate(final DataSource ds,final String sql,final List<Map> rows) {
		TransactionTemplate tt = new TransactionTemplate(new DataSourceTransactionManager(ds));
		return tt.execute(new TransactionCallback<int[]>() {
			@Override
			public int[] doInTransaction(TransactionStatus status) {
				NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(ds);
				return template.batchUpdate(sql, rows.toArray(new Map[rows.size()]));
			}
		});
	}
	
}
