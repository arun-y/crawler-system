package com.arunwizz.crawlersystem.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtil {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CommonUtil.class);

	public static final byte[] getMD5EncodedDigest(String message) {
		byte[] bytesOfMessage = null;
		MessageDigest md = null;
		try {
			bytesOfMessage = message.getBytes("UTF-8");
			md = MessageDigest.getInstance("MD5");
		} catch (UnsupportedEncodingException e) {
			LOGGER.error(e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			LOGGER.error(e.getMessage());
		}
		byte[] digest = md.digest(bytesOfMessage);
		return Base64.encodeBase64URLSafe(digest);
	}

}
