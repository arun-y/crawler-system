package com.arunwizz.crawlersystem.application.pageparser;

import java.io.InputStream;

import org.w3c.dom.Document;

public interface HTMLParser {
    //public OutputStream parse(InputStream pageStream) throws Exception; 
    public Document parse(InputStream pageStream) throws Exception;
}
