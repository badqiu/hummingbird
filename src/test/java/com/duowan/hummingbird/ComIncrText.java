package com.duowan.hummingbird;

import org.junit.Test;

public class ComIncrText {

	public static double years(double base,double incrPercent,int years) {
		double result = base;
		for(int i = 0; i < years; i++) {
			result = (result ) + result * incrPercent;
		}
		return result;
	}
	
	@Test
	public void test() {
		int base = 100000;
		double incrPercent = 0.10;
		System.out.println("基数:"+base);
		System.out.println("年增长率:"+incrPercent+"\n\n");
		for(int i = 1; i <= 20; i++) {
			System.out.println("年数:"+i+" 金额:" +(long)years(base,incrPercent,i));
		}
	}
	
}
