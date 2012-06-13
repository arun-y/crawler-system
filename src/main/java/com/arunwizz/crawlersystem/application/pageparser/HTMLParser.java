package com.arunwizz.crawlersystem.application.pageparser;

import java.io.File;
import java.util.Date;

import org.htmlcleaner.CleanerProperties;
import org.htmlcleaner.DomSerializer;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

public class HTMLParser {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(HTMLParser.class);

    public Document parse(File file) throws Exception {

        Date d1 = new Date();
        LOGGER.info("cleaning html page");
        CleanerProperties props = new CleanerProperties();

        // set some properties to non-default values
        props.setTranslateSpecialEntities(true);
        props.setTransResCharsToNCR(true);
        props.setOmitComments(true);
        props.setNamespacesAware(true);
        props.setOmitXmlDeclaration(false);
        props.setOmitDoctypeDeclaration(false);

        // do parsing
        TagNode tagNode = new HtmlCleaner(props).clean(file);

        LOGGER.trace("cleaned html stream in " + (new Date().getTime() - d1.getTime())/1000.0 + " seconds");
        d1 = new Date();
        LOGGER.debug("converting the cleaned html into w3c Document");
        //convert to w3c DOM
        Document doc = new DomSerializer(props).createDOM(tagNode);
        LOGGER.debug("converted the cleaned html into w3c Document in " + (new Date().getTime() - d1.getTime())/1000.0 + " seconds");
        return doc;

    }    
}
