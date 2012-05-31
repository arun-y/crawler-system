package com.arunwizz.crawlersystem.application.pageparser;

import java.io.InputStream;
import java.util.Date;

import org.htmlcleaner.CleanerProperties;
import org.htmlcleaner.DomSerializer;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

public class HTMLCleanerParser implements HTMLParser {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(HTMLCleanerParser.class);

    @Override
    public Document parse(InputStream pageStream) throws Exception {

        Date d1 = new Date();
        LOGGER.info("cleaning html stream");
        CleanerProperties props = new CleanerProperties();

        // set some properties to non-default values
        props.setTranslateSpecialEntities(true);
        props.setTransResCharsToNCR(true);
        props.setOmitComments(true);
        props.setNamespacesAware(true);
        props.setOmitXmlDeclaration(false);
        props.setOmitDoctypeDeclaration(false);

        // do parsing
        TagNode tagNode = new HtmlCleaner(props).clean(pageStream);

        LOGGER.info("cleaned html stream in " + (new Date().getTime() - d1.getTime())/1000.0 + " seconds");
        d1 = new Date();
        LOGGER.info("converting the cleaned html into w3c Document");
        //convert to w3c DOM
        Document doc = new DomSerializer(props).createDOM(tagNode);
        LOGGER.info("converted the cleaned html into w3c Document in " + (new Date().getTime() - d1.getTime())/1000.0 + " seconds");
        return doc;

    }    
}
