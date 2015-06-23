package com.duowan.hummingbird.db.sql.select;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.springframework.util.ResourceUtils;

public class SelectSqlTest {

	@Test
	public void test() throws FileNotFoundException, IOException {
		SelectSql.parse(getSql("classpath:select_sql/complex.sql"));
//		SelectSql.parse(getSql("classpath:select_sql/if.sql"));
	}

	private String getSql(String file) throws IOException, FileNotFoundException {
		String querySql = FileUtils.readFileToString(ResourceUtils.getFile(file));
		return querySql;
	}

}
