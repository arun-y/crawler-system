package com.arunwizz.crawlersystem.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.core.exception.RobotsCheckerException;

public class RobotsChecker {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(RobotsChecker.class);
	//TODO: define this as configuration/init properties
	private HashMap<String, Set<String>> cache = null;

	public RobotsChecker() {
		cache = new HashMap<String, Set<String>>();
	}

	public boolean isPass(URL url) throws RobotsCheckerException {
		boolean isPass = true;
		try {
			Set<String> disallowPatternSet = cache.get(url.getHost());

			if (disallowPatternSet == null) {
				//TODO: Load the path from configuration, convention over configuration
				Process p = Runtime
						.getRuntime()
						.exec(new String[] {
								"/home/aruny/workspace/crawler_system/target/classes/robots_extractor.py",
								url.getHost() });
				int status = p.waitFor();
				StringBuffer jsonBuffer = new StringBuffer();
				if (status == 0) {
					BufferedReader stdInput = new BufferedReader(
							new InputStreamReader(p.getInputStream()));
					String line = null;
					while ((line = stdInput.readLine()) != null) {
						jsonBuffer.append(line);
					}
					JSONObject json = (JSONObject) JSONSerializer
							.toJSON(jsonBuffer.toString());
					
					disallowPatternSet = new HashSet<String>();
					//check for "CanopusBot" entry
					if (json.containsKey("CanopusBot")){
						//check for "disallow"
						JSONArray disallowPatternJSON = ((JSONArray)((JSONObject)json.get("CanopusBot")).get("da"));
						disallowPatternJSON.addAll(disallowPatternSet);
					} else if (json.containsKey("*")){
						JSONArray disallowPattern = ((JSONArray)((JSONObject)json.get("*")).get("da"));
						disallowPatternSet.addAll(JSONArray.toCollection(disallowPattern, String.class));
					} else {
						LOGGER.info("No robotos disallow pattern found");
					}
					cache.put(url.getHost(),
							disallowPatternSet);
				} else {
					throw new RobotsCheckerException("Error fetching robots.txt");
				}
			} 
			for (String disallowUrlPatter: disallowPatternSet)
			{
				//if disallowPattern begins with give url then fail
				if (url.getPath().indexOf(disallowUrlPatter) == 0) {
					isPass = false;
					break;
				} 
			}
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
			throw new RobotsCheckerException(e);
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage());
			throw new RobotsCheckerException(e);
		} 
		return isPass;
	}
}
