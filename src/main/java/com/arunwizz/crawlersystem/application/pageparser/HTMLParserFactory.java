package com.arunwizz.crawlersystem.application.pageparser;


public class HTMLParserFactory {

    public static int XHTMLDOMParser = 0;
    public static int HTMLCleanerParser = 1;
    

    public static HTMLParser getParser(int parserType) {
        HTMLParser parser = null;
        switch (parserType) {
        case 0:
            parser = new XHTMLParser();
            break;
        case 1:
            parser = new HTMLCleanerParser();
            break;
        }
        return parser;
    }
}
