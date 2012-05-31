package com.arunwizz.crawlersystem.application.ds.tree;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import com.arunwizz.crawlersystem.application.ds.matrix.Matrix;
import com.arunwizz.crawlersystem.application.ds.matrix.MatrixException;

public class Tree<T> implements Comparable<Tree<T>> {

    public final static int PRE_ORDER = 1; 
    public final static int POST_ORDER = 3; 

    public Tree(){
    }
    
    public Tree(Node<T> root){
        this.root = root;
    }
    
    private Node<T> root;
    
    public Node<T> getRoot(){
        return root;
    }
    
    public void setRoot(Node<T> root){
        this.root = root;
    }

    
    public List<Node<T>> traverse(int traverseType) {
        List<Node<T>> traverseList = new ArrayList<Node<T>>();
        switch (traverseType) {
            case PRE_ORDER: preOrder(getRoot(), traverseList);break; 
            case POST_ORDER: postOrder(getRoot(), traverseList);break;
            default: traverseList = null;System.out.println("Invalid traverse type");
        }
        return traverseList;
    }
    
   
    /**
     * recursive function, so be careful for large trees
     * @param element
     * @param list
     */
    private void preOrder(Node<T> element, List<Node<T>> list) {
        list.add(element);
        for (Node<T> data : element.getChildren()) {
            preOrder(data, list);
        }
    }    

    /**
     * recursive function, so be careful for large trees
     * @param element
     * @param list
     */
    private void postOrder(Node<T> element, List<Node<T>> list) {
        list.add(element);
        List<Node<T>> children = element.getChildren();
        for (int i = children.size()-1; i >= 0; i--) {
            postOrder(children.get(i), list);
        }
    }
    
    public int simpleTreeMatching(Tree<T> b) throws MatrixException{
        if (!this.getRoot().getLabel().equals(b.getRoot().getLabel())) {
            b.getRoot().setAligned(true);
            return 0;
        } else {
            int m = this.getRoot().getChildrenSize();
            int n = b.getRoot().getChildrenSize();
            Matrix<Integer> matrix = new Matrix<Integer>(m+1, n+1);
            //start initialize matrix
            for (int i = 0; i <= m; i++) {
                matrix.setElement(i, 0, 0);
            }
            for (int j = 0; j <= n; j++) {
                matrix.setElement(0, j, 0);
            }
            //end initialization matrix
            
            for (int i = 1; i <= m; i++) {
                for (int j = 1; j <= n; j++) {
                    //get sub tree at ith location of matrix A and jth location of matrix B
                    Tree<T> ai = new Tree<T>(this.getRoot().getChildren().get(i-1));
                    Tree<T> bj = new Tree<T>(b.getRoot().getChildren().get(j-1));
                    int wij = ai.simpleTreeMatching(bj);
                    matrix.setElement(i, j, Math.max(Math.max(matrix.getElement(i, j-1), matrix.getElement(i-1, j)), matrix.getElement(i-1, j-1)+wij));
                }
            }
            return matrix.getElement(m, n) + 1;
        }
    }
    
    /**
     * Return the depth of this tree
     * @return
     */
    public int height() {
        return root.height();
    }
    
    /**
     * Returns the sub-tree at specified pre-order location
     * It makes a copy of original tree, so any change in sub-tree 
     * would not impact original tree.
     * 
     * returns null, if specified position is not found
     */
    public Tree<T> getSubTreeByPreOrder(int preOrderPosition) {
        Node<T> subTreeRoot = this.root.getChildAtPreOrderPosition(preOrderPosition);
        //TODO: create a copy
        return new Tree<T>(subTreeRoot);
    }

    /**
     * Returns the sub-tree at specified pre-order location
     * It makes a copy of original tree, so any change in sub-tree 
     * would not impact original tree.
     * 
     * returns null, if specified position is not found
     */
    public Tree<T> getSubTreeByPostOrder(int postOrderPosition) {
        Node<T> subTreeRoot = this.root.getChildAtPreOrderPosition(postOrderPosition);
        //TODO: create a copy
        return new Tree<T>(subTreeRoot);
    }
    
    public int size() {
        return traverse(PRE_ORDER).size();
    }
    
    @Override
    public String toString(){
        StringWriter stringWriter = new StringWriter();
        stringWriter.write(System.getProperty("line.separator"));
        preOrderPrint(root, stringWriter, "");
        return stringWriter.toString();
    }
    
    private void preOrderPrint(Node<T> rootNode, StringWriter stringWriter, String indent) {
        stringWriter.write(indent + "-" + rootNode.getLabel() + "[" + rootNode.getData() + "]" + "{"  + rootNode.getPreOrderPosition() + "}" + System.getProperty("line.separator"));
        for (Node<T> childNode: rootNode.getChildren()) {
            preOrderPrint(childNode, stringWriter, indent + "\t");
        }
    }
    
    @Override
    /**
     * This returns values, such that collection is sorted in descending order
     */
    public int compareTo(Tree<T> o2) {
        if (this.size() < o2.size()) {
            return 1;
        } else if (this.size() > o2.size()) {
            return -1;
        } else {
            return 0;
        }
    }
    
    @Override
    public boolean equals(Object o2) {
        if (o2 == null) {
            return false;
        }
        if (this.toString().equals(o2.toString())) {
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
