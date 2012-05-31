package com.arunwizz.crawlersystem.application.ds.tree;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.arunwizz.crawlersystem.application.ds.matrix.Matrix;


public class Node<T> extends GeneralizedNode<T> {

    /**
     * 
     */
    private static final long serialVersionUID = -6653077195227044067L;
    /**
     * 
     */
    /**
     * Assuming its a tree node, else this should be List for graphs
     */
    private Node<T> parent;
    private Node<T> prevSibling;
    private Node<T> nextSibling;
    private boolean aligned;
    private String label;
    private String data;
    private Map<String, String> attributes;
    transient private Matrix<Float> childDistanceMatrix;
    private ArrayList<DataRegion> dataRegions;
    private List<Node<T>> children;
    private int preOrderPosition;
    private int relativePosition;//relative to siblings
    
    //TODO: remove this constructor, once addChild handles the preorder position
    public Node(int preOrderPosition){
        attributes = new HashMap<String, String>();
        dataRegions = new ArrayList<DataRegion>();
        setPreOrderPosition(preOrderPosition);
    }
    
    public Node(){
        attributes = new HashMap<String, String>();
        dataRegions = new ArrayList<DataRegion>();
        setPreOrderPosition(1);
    }
    
    public Node(String label) {
        this();
        this.label = label;
    }

    //-----------------
    public Node<T> getParent() {
        return this.parent;
    }
    
    public void setParent(Node<T> parent) {
        this.parent = parent;
    }
    //-----------------
    
    public void setPrevSibling(Node<T> prevSibling) {
        this.prevSibling = prevSibling;
    }

    public Node<T> getPrevSibling() {
        return prevSibling;
    }

    public void setNextSibling(Node<T> nextSibling) {
        this.nextSibling = nextSibling;
    }

    public Node<T> getNextSibling() {
        return nextSibling;
    }

    //-----------------
    public String getLabel() {
        return this.label;
    }
 
    public void setLabel(String label) {
        this.label = label;
    }  
    //-----------------

    public void setAligned(boolean aligned) {
        this.aligned = aligned;
    }

    public boolean isAligned() {
        return aligned;
    }

    //-----------------
    public String getData(){
        return this.data;
    }
    
    public void setData(String data){
        this.data = data;
    }
    //-----------------

    //-----------------
    public Map<String, String> getAttributes() {
        return attributes;
    }
    
    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }
    //-----------------
    
    //-----------------    
    public Matrix<Float> getChildDistanceMatrix() {
        return childDistanceMatrix;
    }
    
    public void setChildDistanceMatrix(Matrix<Float> childDistanceMatrix) {
        this.childDistanceMatrix = childDistanceMatrix;
    }
    //-----------------
    
    public void setDataRegions(ArrayList<DataRegion> dataRegions) {
        this.dataRegions = dataRegions;
    }

    public ArrayList<DataRegion> getDataRegions() {
        return dataRegions;
    }

    //-----------------    
    public List<Node<T>> getChildren() {
        if (this.children == null) {
            return new ArrayList<Node<T>>();
        }
        return this.children;
    }
    
    public void setChildren(List<Node<T>> children) {
        this.children = children;
    }
    
    //relative position
    /**
     * it uses 0 based indexing and not 1
     */
    public Node<T> getChildAt(int pos) {
        if (children == null || pos<0 || pos>children.size()-1)
            return null;
        return children.get(pos);
    }
    
    public void setPreOrderPosition(int preOrderPposition) {
        this.preOrderPosition = preOrderPposition;
    }

    public int getPreOrderPosition() {
        return preOrderPosition;
    }

    public void setRelativePosition(int relativePosition) {
        this.relativePosition = relativePosition;
    }
    public int getRelativePosition() {
        return relativePosition;
    }
    public int getChildrenSize() {
        if (children == null) {
            return 0;
        }
        return children.size();
    }
    
    /**
     * currently unused, shall be useful when addChild itself takes care of pre-order position
     * @return
     */
    private int getCurrentPreOrderPosition() {
        if (children == null || children.size() == 0) {
            return this.preOrderPosition;
        } else {
            return children.get(children.size()-1).getCurrentPreOrderPosition();
        }
    }
    
    /**
     * adds a child node to this node
     * @param child
     */
    public void addChild(Node<T> child) {
        if (children == null) {
            children = new ArrayList<Node<T>>();
        }
        child.setRelativePosition(children.size());/*keeps the relative position among siblings*/
        //child.setPreOrderPosition(getCurrentPreOrderPosition()+1);
        /*TODO: ideally, pre-order position for following nodes
        should be updated as well, currently assuming, nodes are added in pre-order fashion only.*/
        child.parent = this;
        if (!children.isEmpty()) {
            child.setPrevSibling(children.get(children.size()-1));
            children.get(children.size()-1).setNextSibling(child);
        }
        children.add(child);
        
    }    
    //----------------------
    
    //----------------------
    /**
     * depth of this node
     */
    public int height(){
        if (getChildrenSize() == 0) {
            return 1;
        } else {
            //max of children heights + 1
            List<Integer> childHeights = new ArrayList<Integer>();
            for (Node<T> child: getChildren()){
                childHeights.add(child.height());
            }
            return Collections.max(childHeights) + 1;
        }
    }
    
    /**
     * returns child node at given pre-order location
     */
    public Node<T> getChildAtPreOrderPosition(int preOrderPosition) {
        if (this.preOrderPosition == preOrderPosition) {
            return this;
        } else {
            for (Node<T> child:getChildren()) {
                Node<T> subNode = child.getChildAtPreOrderPosition(preOrderPosition);
                if (subNode != null) {
                    return subNode;
                } else {
                    continue;
                }
            }
            return null;
        }
    }

    /**
     * returns child node at given post-order location, relative to root
     * index starts from 1
     */
    public Node<T> getChildAtPostOrderPosition(int postOrderPosition) {
        return this.getChildAt(postOrderPosition-1);
    }
    
    @Override
    /**
     * returns pre-order tag string 
     */
    public String toString() {
        
        StringWriter stringWriter = new StringWriter();
        stringWriter.write(System.getProperty("line.separator"));
        preOrderPrint(this, stringWriter, "");
        return stringWriter.toString();        
        
    }
    
    public String toPreOrderString() {
        StringWriter sw = new StringWriter();
        sw.append(this.label.toString());
        for (Node<T> child: getChildren()){
            sw.append(child.toPreOrderString());
        }
        return sw.toString();
    }
    
    private void preOrderPrint(Node<T> rootNode, StringWriter stringWriter, String indent) {
        stringWriter.write(indent + "-" + rootNode.getLabel() + "[" + rootNode.getData() + "]" + "{"  + rootNode.getPreOrderPosition() + "}" + System.getProperty("line.separator"));
        for (Node<T> childNode: rootNode.getChildren()) {
            preOrderPrint(childNode, stringWriter, indent + "\t");
        }
    }    
    
    /**
     * return a deep clone/copy of current node
     */
    public Node<T> copy() {
        Node<T> copiedNode = new Node<T>(this.label);
        //TODO: we might need it
        return copiedNode;
    }
}
