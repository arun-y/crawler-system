package com.arunwizz.crawlersystem.core;

import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Test;

import com.arunwizz.crawlersystem.core.exception.RobotsCheckerException;

public class RobotsCheckerTest {

	
	@Test
	public void testIsPass() throws MalformedURLException, RobotsCheckerException {
		boolean isPass = true;
		RobotsChecker rc = new RobotsChecker();
		isPass = rc.isPass(new URL("http", "www.flipkart.com", "/the-secret-1847370292/p/itmczzsagpbeznuy"));
		assertEquals(true, isPass);

		isPass = rc.isPass(new URL("http", "www.flipkart.com", "/search/a/all?query=matrix&vertical=all&dd=0&autosuggest%5Bas%5D=off&autosuggest%5Bas-submittype%5D=entered&autosuggest%5Bas-grouprank%5D=0&autosuggest%5Bas-overallrank%5D=0&Search=%C2%A0&_r=n_2yuAC4xgh0SZTuulvAtw--&_l=Tnndui8JdMVk7CZmDKIfXQ--&ref=93be57f5-8840-41df-91bf-a44bb48afdcf&selmitem="));
		assertEquals(false, isPass);

		isPass = rc.isPass(new URL("http", "www.ebay.in", "/help/confidence/"));
		assertEquals(false, isPass);
		
			
		
	}

}
