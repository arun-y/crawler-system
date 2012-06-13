package com.arunwizz.crawlersystem.application;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;

import com.arunwizz.crawlersystem.application.ds.tree.GeneralizedNode;
import com.arunwizz.crawlersystem.application.ds.tree.Node;
import com.arunwizz.crawlersystem.application.ds.tree.Tree;
import com.arunwizz.crawlersystem.application.ds.tree.TreeUtil;
import com.arunwizz.crawlersystem.application.pageparser.HTMLParser;

public class HTMLPageCrawler {
    
    private static Log logger = LogFactory.getLog(HTMLPageCrawler.class);

    public static void main(String argv[]) throws Exception {
        HTMLParser pd = null;//HTMLParserFactory.getParser(HTMLParserFactory.HTMLCleanerParser);//SEEMS GOOD
        //HTMLParser pd = HTMLParserFactory.getParser(HTMLParserFactory.XHTMLDOMParser);
        
        Date d1 = new Date();
        logger.info("getting page at " + d1.getTime());
//        InputStream pageStream = PageLoader.loadPage("http://www.w3.org/TR/xhtml1/");
//        URI uri = new URI("http", "www.flipkart.com", "/samsung-galaxy-tab-p1000-chic-white-mobile-mobcttvwhfdckq4w", null); 
//        URI uri = new URI("http", "www.buytheprice.com", "/mobiles/viewmore.php?id=1124", null); 
//        URI uri = new URI("http", "www.amazon.com", "/gp/product/B003YH9DZ4/ref=s9_simh_gw_p23_d4_i4?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=center-4&pf_rd_r=0BN76QAQHK3FVH3Z3TZZ&pf_rd_t=101&pf_rd_p=470939031&pf_rd_i=507846", null); 
        URI uri = new URI("http", "www.flipkart.com", "/mobiles/blackberry/curve-8520-itmczq5bgywsy2mg", null); 
//        URI uri = new URI("http", "utvmoney.mangopeople.com", "/companies/nifty/NIFTY/options/call/", null); 
//        URI uri = new URI("http", "www.letsbuy.com", "/electronics/tv/led-tv/lg/lg-22le5300-led-lcd-tv-p-12548.html?osCsid=ts61bc3jq35a2r14k8tilktoi7", null); 
//        URI uri = new URI("file", "testPages", "/onifty.html", null); 
//        URI uri = new URI("http", "www.nseindia.com", "/marketinfo/fo/fomwatchsymbol.jsp?key=NIFTY", null); 
        String url = uri.toASCIIString();
        url = url.replaceFirst("%3[f|F]", "?");//some servers couldn't understand encoded ? as query separator.
        logger.info("Encoded Url: " + url);
        //TODO:
        InputStream pageStream = null;//PageLoader.loadPage(url);
//        InputStream pageStream = PageLoader.loadPage("http://www.chinadaily.com.cn/index.html");
        
//        InputStream pageStream = PageLoader.loadPage("file://testPages/canon.html");//FAILED 
//        InputStream pageStream = PageLoader.loadPage("file://testPages/kodak.html");//WILL PASS
//        InputStream pageStream = PageLoader.loadPage("file://testPages/kodak1.html");//FAILED
//        InputStream pageStream = PageLoader.loadPage("file://testPages/nikon.html");//FAILED
//        InputStream pageStream = PageLoader.loadPage("file://testPages/polaroid.html");//failed
//        InputStream pageStream = PageLoader.loadPage("file://testPages/sanyo1.html");//failed
//        InputStream pageStream = PageLoader.loadPage("file://testPages/sony.html");//failed - not strict xhtml
        if (pageStream != null) {
            logger.info("got page in " + (new Date().getTime() - d1.getTime())/1000.0 + " seconds");
            d1 = new Date();
            BufferedInputStream stream = new BufferedInputStream(pageStream);
            
            int a = -1;
            while ((a = stream.read()) != -1) {
                System.out.print((char)a);
            }
            
            logger.info("parsing  page at " + d1.getTime());
            Document pageDomTree = null;//pd.parse(stream);
            
            logger.info(pageDomTree);
            
            logger.info("parsed page in " + (new Date().getTime() - d1.getTime())/1000.0 + " seconds");
            
            TreeUtil tutil = new TreeUtil();
            Tree<String> pageTree = tutil.getTreeFromDOM(pageDomTree);
            
            logger.info(pageTree);
            
            //Call MDR and FINDDR to identify all data regions
            int k = 10;//maximum combination
            float t = 0.3f;//edit distance threshold
            tutil.mdr(pageTree.getRoot(), k);
            tutil.findDR(pageTree.getRoot(), k, t);
            List<List<GeneralizedNode<Node<String>>>> drList = tutil.getDRs(pageTree);
            logger.info(drList.size());
            logger.info(drList);
            
            
            
            for (List<GeneralizedNode<Node<String>>> dr: drList) {
                /*for each data region */
                if (dr.get(0).size() == 1) {
                    tutil.findRecord1(dr.get(0));// Case 1
                } else {
                    tutil.findRecordN(dr.get(0));//Case 2
                }
            }

            //Data Extraction from DEPTA Section 4, Page# 80.
            /*
             * Produce one rooted tag tree for each data records for each data region.
             */
            for (List<GeneralizedNode<Node<String>>> dr: drList) {
                /*for each data region */
                PriorityQueue<Tree<String>> dataRecordQueue = tutil.buildDataRecrodTree(dr);
                tutil.partialTreeAlignment(dataRecordQueue);
            }
            
            logger.info(pageTree);
            
            Tree<String> temp = pageTree.getSubTreeByPreOrder(55);
            System.out.println(temp);

            /*
            Source source = new DOMSource(pageDomTree);
            StringWriter stringWriter = new StringWriter();
            Result result = new StreamResult(stringWriter);
            TransformerFactory factory = TransformerFactory.newInstance();
            Transformer transformer = factory.newTransformer();
            transformer.transform(source, result);
            logger.info(stringWriter.getBuffer().toString());          
             */
        }
        // pd.setUrl("http://www.amazon.com/s/ref=amb_link_85318851_27?ie=UTF8&node=565108&field-price=00000-39999&field-availability=-1&emi=ATVPDKIKX0DER&pf_rd_m=ATVPDKIKX0DER&pf_rd_s=center-6&pf_rd_r=143DDSYDRCRZ6D7DMYDQ&pf_rd_t=101&pf_rd_p=1288331602&pf_rd_i=565108");
        // pd.setUrl("http://www.w3.org/TR/xhtml1/");
    }
}