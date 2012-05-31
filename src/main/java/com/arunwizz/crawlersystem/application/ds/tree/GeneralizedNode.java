package com.arunwizz.crawlersystem.application.ds.tree;

import java.util.ArrayList;

/**
 * An artificial node for one more collection of tag node
 * @author Arun_Yadav
 *
 */
public class GeneralizedNode<T> extends ArrayList<T> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    public DataRecordIndecator dataRecordIndecator; 
    
    public enum DataRecordIndecator {SELF, CHILD_CONT, CHILD_NON_CONT};    

    public boolean addNode(T node){
        return super.add(node);
    }
    

}
