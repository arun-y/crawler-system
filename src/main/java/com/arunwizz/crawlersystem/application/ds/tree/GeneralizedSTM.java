package com.arunwizz.crawlersystem.application.ds.tree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.arunwizz.crawlersystem.application.ds.matrix.Matrix;
import com.arunwizz.crawlersystem.application.ds.matrix.MatrixException;

public class GeneralizedSTM {

    public static class Tuple<T> extends ArrayList<T> {

        private static final long serialVersionUID = 1L;
        public Tuple(int n) {
            super(n);
        }
    }
    
    private static class DetectListTuple extends Tuple<Object> {

        private static final long serialVersionUID = 1L;
        public DetectListTuple(Matrix<MatchTuple> wMatrix, Integer nodesA, Integer nodesB) {
            super(3);
            set(0, wMatrix);
            set(1, nodesA);
            set(2, nodesB);
        }
        
        public Matrix<MatchTuple> matchTuple() {
            return (Matrix<MatchTuple>)get(0);
        }
        
        public int nodesA() {
            return (Integer)get(1);
        }

        public int nodesB() {
            return (Integer)get(1);
        }
    }
    
    private static class MatchTuple extends Tuple<Integer>{

        private static final long serialVersionUID = 1L;
        public MatchTuple(int score, int nodesA, int nodesB) {
            super(3);
            set(0, score);
            set(1, nodesA);
            set(2, nodesB);
        }
        
        public int score() {
            return get(0);
        }
        
        public int nodesA() {
            return get(1);
        }
        
        public int nodesB() {
            return get(2);
        }
    }

    public MatchTuple match(Tree<String> a, Tree<String> b) {

        if (!a.getRoot().getLabel().equalsIgnoreCase(b.getRoot().getLabel())) {
            return new MatchTuple(0, a.size(), b.size());
        } else {
            int m = a.getRoot().getChildrenSize();
            int n = b.getRoot().getChildrenSize();

            // start initialize match matrix
            Matrix<Integer> matchMatrix = new Matrix<Integer>(m + 1, n + 1);
            for (int i = 0; i <= m; i++) {
                matchMatrix.setElement(i, 0, 0);
            }
            for (int j = 0; j <= n; j++) {
                matchMatrix.setElement(0, j, 0);
            }
            // end initialization match matrix

            // start W matrix
            Matrix<MatchTuple> wMatrix = new Matrix<MatchTuple>(m, n);

            for (int i = 1; i <= m; i++) {
                for (int j = 1; j <= n; j++) {
                    wMatrix.setElement(
                            i,
                            j,
                            match(new Tree<String>(a.getRoot()
                                    .getChildAt(i - 1)), new Tree<String>(b
                                    .getRoot().getChildAt(j - 1))));
                }
            }
            DetectListTuple detectListTuple = detectLists(wMatrix, a, b);
            wMatrix = detectListTuple.matchTuple();
            //compute scores
            for (int i = 1; i <= m; i++) {
                for (int j = 1; j <= n; j++) {
                    matchMatrix.setElement(i, j, Math.max(Math.max(
                            matchMatrix.getElement(i, j - 1),
                            matchMatrix.getElement(i - 1, j)),
                            matchMatrix.getElement(i - 1, j - 1) + wMatrix.getElement(i, j).score()));
                }
            }
            return new MatchTuple(matchMatrix.getElement(m, n)+1, detectListTuple.nodesA(), detectListTuple.nodesB());
        }
    }

    /**
     * Updates passes wMatirx as per the list discovered 
     * @param a
     * @param b
     * @return Matrix<MatchTuple>
     */
    private DetectListTuple detectLists(Matrix<MatchTuple> wMatrix, Tree<String> a, Tree<String> b) throws MatrixException {
        int m = wMatrix.getRowSize();
        int n = wMatrix.getColSize();
        Matrix<Double> normWMatrix = new Matrix<Double>(m, n);
        //compute normalized match value
        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= n; j++) {
                MatchTuple matchTuple = wMatrix.getElement(n, j);
                normWMatrix.setElement(i, j, (double)matchTuple.score()/(Math.max(matchTuple.nodesA(), matchTuple.nodesB())));
            }
        }       
        double maxAi = 0, maxBj = 0;
        double t1 = 50.0, t2 = 70.0;
        
        List<String> stringA = new ArrayList<String>();
        List<String> stringB = new ArrayList<String>();
        
        for (int i = 1; i <= m; i++) {
            maxAi =   Collections.max(normWMatrix.getRow(i-1)); 
            for (int j = 1; j <= n; j++) {
                double normScore = normWMatrix.getElement(i, j);
                maxBj = Collections.max(normWMatrix.getColumn(j-1));
                if (normScore > t1 && (normScore/maxAi > t2) && (normScore/maxBj > t2)) {//#10
                    /*Sub tree Sai and Sbj matches*/
                    //TODO: See if symbol table is required?? Required
                    //#14
                    Tree<String> sai = a.getSubTreeByPreOrder(i);
                    stringA.add(a.getRoot().getChildAt(i-1).getLabel());

                    Tree<String> sbi = b.getSubTreeByPreOrder(j);
                    stringB.add(b.getRoot().getChildAt(i-1).getLabel());
                } else {
                    stringA.add(a.getRoot().getChildAt(i-1).getLabel());
                    stringB.add(b.getRoot().getChildAt(i-1).getLabel());
                }
            }
        }
        String gA = grammarGeneration(stringA);
        String gB = grammarGeneration(stringB);
        if (matchGrammerString(gA, gB)) {
            return updateW(wMatrix, gA, gB);
        } else {
            return new DetectListTuple(wMatrix, a.size(), b.size());
        }
        
        
    }

    //as per match definition on pg. 934, section 4.3 para 6.
    private boolean matchGrammerString(String gA, String gB) {
        String[] gASplits = gA.split(":");
        String[] gBSplits = gB.split(":");
        gA = "";
        gB = "";
        for (String gASplit: gASplits) {
            if (gASplit.charAt(gASplit.length()-1) == '+'){
                gA.concat(gASplit);
            }
        }
        for (String gBSplit: gBSplits) {
            if (gBSplit.charAt(gBSplit.length()-1) == '+'){
                gB.concat(gBSplit);
            }
        }
        return gA.equals(gB);                
    }
    
    /**
     * 
     * @param wMatrix
     * @param gA
     * @param gB
     * @return
     */
    private DetectListTuple updateW(Matrix<MatchTuple> wMatrix, String gA,
            String gB) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * 
     * @param string
     * @return
     */
    private String grammarGeneration(List<String> strings) {
        return null;
    }
    
    private short[] symbolList = {};
    
    private String getSymbol(String treeLabel) {
        Map<String, String> symbolTable = new HashMap<String, String>();
        String symbol = symbolTable.get(treeLabel);
        if (symbol == null) {
            symbol = "";//TODO: get new symbol
            symbolTable.put(treeLabel, symbol);
        } 
        return symbol;
    }

}
