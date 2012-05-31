package com.arunwizz.crawlersystem.application.pageparser;

import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;

public class XHTMLParser implements HTMLParser {

    
    @Override
    public Document parse(InputStream pageStream) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        Document document = builder.parse(pageStream);
        return document;
    }
}
