package com.arunwizz.crawlersystem.application.ds.tree;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.traversal.DocumentTraversal;
import org.w3c.dom.traversal.NodeFilter;
import org.w3c.dom.traversal.TreeWalker;

import com.arunwizz.crawlersystem.application.ds.matrix.Matrix;
import com.arunwizz.crawlersystem.application.ds.tree.GeneralizedNode.DataRecordIndecator;

/**
 * Thread unsafe utility class
 * 
 * @author Arun_Yadav
 * 
 */
public class TreeUtil {

    private Log logger = LogFactory.getLog(TreeUtil.class);

    public Tree<String> getTreeFromDOM(Document document) {
        DocumentTraversal traversal = (DocumentTraversal) document;
        TreeWalker walker = traversal.createTreeWalker(
                document.getDocumentElement(), NodeFilter.SHOW_ELEMENT, null,
                true);
        int[] nodePosition = new int[] { 1 };
        Tree<String> tree = new Tree<String>(traverseDom(walker, nodePosition));
        return tree;
    }

    private Node<String> traverseDom(TreeWalker walker, int[] nodePosition) {
        Node<String> rootNode = new Node<String>(nodePosition[0]);
        org.w3c.dom.Node currentNode = walker.getCurrentNode();
        rootNode.setLabel(currentNode.getNodeName());
        rootNode.setData(getNodeValue(currentNode));

        for (org.w3c.dom.Node n = walker.firstChild(); n != null; n = walker
                .nextSibling()) {
            if (n.getNodeName().equalsIgnoreCase("script")
                    || n.getNodeName().equalsIgnoreCase("noscript")
                    || n.getNodeName().equalsIgnoreCase("style")
                    || n.getNodeName().equalsIgnoreCase("head")
                    || n.getNodeName().equalsIgnoreCase("br")
                    || n.getNodeName().equalsIgnoreCase("input")
                    || n.getNodeName().equalsIgnoreCase("hr")) {
                continue;
            }
            
            nodePosition[0] += 1;
            rootNode.addChild(traverseDom(walker, nodePosition));
        }
        walker.setCurrentNode(currentNode);
        return rootNode;
    }

    private String getNodeValue(org.w3c.dom.Node node) {
        NodeList nodeList = node.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (nodeList.item(i).getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
                return nodeList.item(i).getNodeValue().trim();
            }
        }
        // if given element node has no text child, return null
        return null;
    }

    public void findDR(Node<String> node, int k, float t) {
        if (node.height() >= 3) {// 1
            node.setDataRegions(indentifyDRs(0, node, k, t));// 2
            ArrayList<DataRegion> tempDRs = new ArrayList<DataRegion>();// 3
            for (Node<String> child : node.getChildren()) {// 4
                findDR(child, k, t);// 5
                ArrayList<DataRegion> unCoversDRs = unCovererDRs(node, child);
                if (unCoversDRs != null && !unCoversDRs.isEmpty()) {
                    tempDRs.addAll(unCoversDRs);
                }
            }
            node.getDataRegions().addAll(tempDRs);// 7
        }
    }

    private ArrayList<DataRegion> indentifyDRs(int start, Node<String> node,
            int k, float t) {
        ArrayList<DataRegion> idenDRs = new ArrayList<DataRegion>();

        DataRegion maxDR = new DataRegion(0, 0, 0, 0);// 1
        DataRegion curDR = new DataRegion(0, 0, 0, 0);// 8
        for (int j = 1; j <= Math.min(k, node.getChildrenSize() / 2); j++) {/*
                                                                             * for
                                                                             * each
                                                                             * j
                                                                             * -
                                                                             * combination
                                                                             */// 2//think
                                                                               // need
                                                                               // to
                                                                               // prevent
                                                                               // crossing
                                                                               // combination
                                                                               // more
                                                                               // than
                                                                               // child
                                                                               // count
            for (int f = start; f <= start + j; f++) {/* start from each node */
                boolean flag = true;
                for (int i = f; i < node.getChildrenSize() - 1; i += j) {
                    Float distanceij = node.getChildDistanceMatrix()
                            .getElement(j - 1, i);
                    int currentChildNodePreOrderPosition = node.getChildAt(i)
                            .getPreOrderPosition();
                    if (distanceij != null && distanceij < t) {
                        if (flag) {
                            curDR = new DataRegion(
                                    currentChildNodePreOrderPosition, i, j,
                                    2 * j);// all index start with 0
                            flag = false;
                        } else {
                            curDR.setNodeCount(curDR.getNodeCount() + j);
                        }
                    } else if (!flag) {
                        break;// 11: exit inner for loop
                    }
                }// inner most for
                if (maxDR.getNodeCount() < curDR.getNodeCount()
                        && (maxDR.getRegionStartRelativePosition() == 0 || maxDR
                                .getNodeCount() >= curDR.getNodeCount())) {
                    maxDR = curDR;
                }
            }// middle for
        }// outer for
        if (maxDR.getNodeCount() != 0) {
            if (maxDR.getRegionStartRelativePosition() + maxDR.getNodeCount() != node
                    .getChildrenSize()) {
                idenDRs.addAll(indentifyDRs(
                        maxDR.getRegionStartRelativePosition()
                                + maxDR.getNodeComb(), node, k, t));
            } else {
                idenDRs.add(maxDR);
            }
        }
        return idenDRs;
    }

    private ArrayList<DataRegion> unCovererDRs(Node<String> node,
            Node<String> child) {
        for (DataRegion dr : node.getDataRegions()) {
            if (child.getRelativePosition() >= dr
                    .getRegionStartRelativePosition()
                    && child.getRelativePosition() <= dr
                            .getRegionStartRelativePosition()
                            + dr.getNodeCount()) {// 2
                return null;
            }
        }
        return child.getDataRegions();
    }

    public void mdr(Node<String> node, int k) {
        if (node.height() >= 3) {
            combComp(node, k);
            for (Node<String> child : node.getChildren()) {
                mdr(child, k);
            }
        }
    }

    /**
     * Refer to Figure 6: The structure comparison algorithm, pg# 5 of
     * "Mining data records of web pages" by, Bing Liu
     * 
     * @param children
     * @param maxComb
     */
    private void combComp(Node<String> node, int maxComb) {
        List<Node<String>> children = node.getChildren();
        /* initialize the node to store the edit distance among child */
        /*
         * the row of the matrix will store the i-th child and, and column for
         * every j-combination
         */
        node.setChildDistanceMatrix(new Matrix<Float>(maxComb, children.size()));
        /* for each node */
        for (int i = 0; i < Math.min(maxComb, node.getChildrenSize() / 2); i++) {// added
                                                                                 // child.size/2
                                                                                 // to
                                                                                 // avoid
                                                                                 // computing
                                                                                 // edit
                                                                                 // distance
                                                                                 // for
                                                                                 // i
                                                                                 // >
                                                                                 // child
                                                                                 // size
            /* for each combination */
            for (int j = i + 1; j <= maxComb
                    && (i + 2 * j - 1) < children.size(); j++) {
                /*
                 * check if at least one pair of combination can be formed,
                 * within length
                 */
                // if ((i+2*j-1) < children.size()) {
                int st = i;
                /*
                 * start with first combination, and jump to next combination
                 * within length
                 */
                for (int k = i + j; k < children.size(); k += j) {
                    /* check if next combination can be made within length */
                    if (k + j - 1 < children.size()) {
                        float nd = normalizedEditDistance(children, st, k, k
                                + j - 1);
                        node.getChildDistanceMatrix().setElement(j - 1, st, nd);// at
                                                                                // jth
                                                                                // combination
                                                                                // for
                                                                                // ith
                                                                                // node
                        st = k;
                    }
                }
                // }
            }
        }
    }

    /**
     * Partial Tree Alignment for data extraction
     */
    public void partialTreeAlignment(PriorityQueue<Tree<String>> sQ) {
        Tree<String> ts = sQ.poll();
        boolean flag = false;
        PriorityQueue<Tree<String>> rQ = new PriorityQueue<Tree<String>>();
        boolean i = false;// all unaligned items inserted
        while (!sQ.isEmpty()) {
            Tree<String> ti = sQ.poll();
            int matcheCount = ts.simpleTreeMatching(ti);// matches and aligns
            if (matcheCount < ti.size()) {/* if not node aligned */
                i = insertIntoSeed(ts, ti);
                if (!i) {/* if still not all aligned */
                    rQ.add(ti);
                }
            }
            if (matcheCount > 0 || i) {
                flag = true;
            }
            if (sQ.isEmpty() && flag) {
                sQ = rQ;
                rQ = new PriorityQueue<Tree<String>>();
                flag = false;
                i = false;
            }
        }
        // TODO: output data item from each ti to the data table
        //logger.info(arg0)
    }

    private boolean insertIntoSeed(Tree<String> ts, Tree<String> ti) {
        //TODO: keep it for end.. but important
        /* start with each child */
        ArrayList<Node<String>> unalignedNodes = new ArrayList<Node<String>>();
        Node<String> child = ti.getRoot().getChildAt(0);
        while (child != null) {
            if (!child.isAligned()) {
                unalignedNodes.add(child);
                child = child.getNextSibling();
            } else {
                //check if both left and right nodes exists to unaligned nodes
                //if yes, check if corresponding nodes are immediate neighbors in Ts
                Node<String> leftNode = unalignedNodes.get(0).getPrevSibling();
                Node<String> rightNode = unalignedNodes.get(unalignedNodes.size()-1).getNextSibling();
                
                break;//break as soon as found next aligned child.
            }
        }
        
        return true;
    }

    /**
     * Identifying data records for generalized node of size > 1
     */
    public void findRecordN(GeneralizedNode<Node<String>> generalizedNode) {

        /* Get all the components of given generalized node */
        Iterator<Node<String>> gi = generalizedNode.iterator();
        boolean similarChildren = false;
        while (gi.hasNext()) {
            Node<String> tagNode = gi.next();
            
            if (gi.hasNext()) {/* if this is not the last tagnode */
                Node<String> nextTagNode = tagNode.getNextSibling();
                if (tagNode.getChildrenSize() != nextTagNode.getChildrenSize()) {
                    /* generalized node itself is data record */
                    //generalizedNode.dataRecordIndecator = DataRecordIndecator.SELF;
                    similarChildren = false;
                    break;/*no need to test further, g is data record itself*/
                }
            }
            Iterator<Node<String>> childIter = tagNode.getChildren().iterator();
            if (childIter.hasNext()) {
                /*
                 * at least one child
                 */
                Node<String> child = childIter.next();
                if (!childIter.hasNext()) {
                    /*
                     * single child, treat as all child similar
                     */
                    similarChildren = true;
                } else {/*more than one child available*/
                    Node<String> nextChild = childIter.next();
                    if (child.toPreOrderString().equalsIgnoreCase(nextChild.toPreOrderString())) {
                        similarChildren = true;
                        while (childIter.hasNext()) {
                            child = nextChild;
                            nextChild = childIter.next();
                            if (child.toPreOrderString().equalsIgnoreCase(
                                    nextChild.toPreOrderString())) {
                                similarChildren = true;
                            } else {
                                similarChildren = false;
                                break;// no need to check further, treat as non
                                      // similar
                            }
                        }
                    }
                }
            }
        }
        if (similarChildren) {
            /*
             * records are non contiguous just indicate this on 
             * generalized node, the same should be assumed for remaining
             * generalized node
             */
            generalizedNode.dataRecordIndecator = DataRecordIndecator.CHILD_NON_CONT;

        } else {
            generalizedNode.dataRecordIndecator = DataRecordIndecator.SELF;
        }
    }

    /**
     * Identifying data records for generalized node of size == 1
     */
    public void findRecord1(GeneralizedNode<Node<String>> generalizedNode) {
        // get the alone child
        Node<String> tagNode = generalizedNode.get(0);
        // check if all the child nodes are similar
        boolean similarChildren = false;
        Iterator<Node<String>> childIter = tagNode.getChildren().iterator();
        if (childIter.hasNext()) {
            /*
             * at least one child
             */
            Node<String> child = childIter.next();
            if (!childIter.hasNext()) {
                /*
                 * single child, treat as all child similar
                 */
                similarChildren = true;
            } else {
                Node<String> nextChild = childIter.next();
                if (child.toPreOrderString().equalsIgnoreCase(nextChild.toPreOrderString())) {
                    similarChildren = true;
                    while (childIter.hasNext()) {
                        child = nextChild;
                        nextChild = childIter.next();
                        if (child.toPreOrderString().equalsIgnoreCase(
                                nextChild.toPreOrderString())) {
                            similarChildren = true;
                        } else {
                            similarChildren = false;
                            break;// no need to check further, treat as non
                                  // similar
                        }
                    }
                }
            }
        }
        if (!tagNode.getLabel().equalsIgnoreCase("tr") && similarChildren) {
            generalizedNode.dataRecordIndecator = Node.DataRecordIndecator.CHILD_CONT;
        } else {
            generalizedNode.dataRecordIndecator = Node.DataRecordIndecator.SELF;
        }
    }

    public PriorityQueue<Tree<String>> buildDataRecrodTree(
            List<GeneralizedNode<Node<String>>> dr) {
        PriorityQueue<Tree<String>> dataRecrodQueue = new PriorityQueue<Tree<String>>();
        DataRecordIndecator di = null;
        for (int i = 0; i < dr.size(); i++) {
            GeneralizedNode<Node<String>> g = dr.get(i);
            if (i == 0) {/*
                          * if its first generalized node, read the information
                          * about data record
                          */
                di = g.dataRecordIndecator;
            }
            if (g.size() == 1) {
                switch (di) {
                case SELF:
                    dataRecrodQueue.add(new Tree<String>(g.get(0)));
                    break;
                case CHILD_CONT:
                    for (Node<String> child : g.get(0).getChildren()) {
                        dataRecrodQueue.add(new Tree<String>(child));
                        // make sub
                        // tree for
                        // data
                        // record
                    }
                    break;
                case CHILD_NON_CONT:
                    //TODO: THIS SHOULD NOT HAPPEN
                    break;
                }
            } else /*multiple tag nodes*/ {
                // TODO: not implemented completely, test findRecordN first
                Tree<String> tagTree = new Tree<String>(new Node<String>(
                "p"));
                switch (di) {
                case SELF:
                    for (Node<String> self : g) {
                        tagTree.getRoot().addChild(self);
                    }
                    dataRecrodQueue.add(tagTree);
                    break;
                case CHILD_CONT:
                    //TODO: THIS SHOULD NOT HAPPEN
                    break;
                case CHILD_NON_CONT:
                    for (int j = 0; j < g.get(0).getChildrenSize(); j++) {/*for each child*/
                        tagTree = new Tree<String>(new Node<String>(
                        "p")); 
                        for (Node<String> tagNode: g){
                            tagTree.getRoot().addChild(tagNode.getChildAt(j));
                        }
                        dataRecrodQueue.add(tagTree);
                    }
                    break;
                }
            }
        }
        return dataRecrodQueue;
    }

    /**
     * Return list of rooted Tree for every data records in all data regions.
     * 
     * @return
     */
    public List<List<GeneralizedNode<Node<String>>>> getDRs(
            Tree<String> pageTree) {

        /*
         * A sorted set of all data record sub tree, size of tree will be used
         * for sorting
         */
        List<List<GeneralizedNode<Node<String>>>> drList = new ArrayList<List<GeneralizedNode<Node<String>>>>();

        for (DataRegion dr : pageTree.getRoot().getDataRegions()) {// for each
                                                                   // data
                                                                   // region
            List<GeneralizedNode<Node<String>>> generalizedNodeList = new ArrayList<GeneralizedNode<Node<String>>>();
            Node<String> tagNode = null;
            for (int i = 0; i < dr.getNodeCount() / dr.getNodeComb(); i++) {// for
                                                                            // each
                                                                            // generailzed
                                                                            // node
                GeneralizedNode<Node<String>> g = new GeneralizedNode<Node<String>>();
                // Tree<String> tempGeneralizedNodeTree = new Tree<String>(new
                // Node<String>("g"));//dummy root for generalized node
                for (int j = 0; j < dr.getNodeComb(); j++) {// for each tag node
                    if (i == 0 && j == 0) {/*
                                            * first tag tree of first
                                            * generalized node
                                            */
                        tagNode = pageTree.getSubTreeByPreOrder(
                                dr.getRegionStartPreOrderPosition()).getRoot();
                        g.add(tagNode);
                    } else {/* remaining tag tree is sibling */
                        tagNode = pageTree.getSubTreeByPreOrder(
                                tagNode.getNextSibling().getPreOrderPosition())
                                .getRoot();
                        g.add(tagNode);
                    }
                }
                generalizedNodeList.add(g);
            }
            drList.add(generalizedNodeList);
        }
        return drList;
    }

    /**
     * computes normalized edit distance between two sub tree strings. The
     * arguments are positional index based on counting starting from 1, so we
     * need to reduce by 1, as Java list counting start with 0
     * 
     * @param children
     * @param st
     *            - first combination start position
     * @param k
     *            - next combination start position
     * @param en
     *            - next combination end position
     */
    private float normalizedEditDistance(List<Node<String>> children, int st,
            int k, int en) {
        List<Node<String>> firstChildList = children.subList(st, k);
        List<Node<String>> secondChildList = children.subList(k, en + 1);
        StringWriter swFirst = new StringWriter();
        for (Node<String> child : firstChildList) {
            swFirst.append(child.toPreOrderString());
        }
        String str1 = swFirst.toString();
        StringWriter swSecond = new StringWriter();
        for (Node<String> child : secondChildList) {
            swSecond.append(child.toPreOrderString());
        }
        String str2 = swSecond.toString();

        Date d1 = new Date();
        int editDistance = xlevenshteinDistance(str1, str2);
        logger.debug("xeditDistance:" + editDistance + "["
                + (new Date().getTime() - d1.getTime()) + " ms]");

        // d1 = new Date();
        // logger.info("leditDistance:" + levenshteinDistance(str1, str2) + "["
        // + (new Date().getTime() - d1.getTime()) + " ms]");
        // logger.info("----");

        /*
         * normalized edit distance = edit distance divided by mean length of
         * two strings
         */
        BigDecimal meanLength = new BigDecimal(str1.length() + str2.length()
                / 2.0);
        BigDecimal normalizedEditDistance = new BigDecimal(editDistance)
                .divide(meanLength, 1, RoundingMode.HALF_UP);
        return normalizedEditDistance.floatValue();
    }

    /**
     * Based on LevenshteinDistance algorithm
     * 
     * @param firstString
     * @param secondString
     * @return
     */
    public int levenshteinDistance(String str1, String str2) {
        // logger.debug("computing levenshtein distance for strings " + str1 +
        // " and " + str2);
        Matrix<Integer> distanceMatrix = new Matrix<Integer>(str1.length() + 1,
                str2.length() + 1);

        for (int i = 0; i <= str1.length(); i++) {
            distanceMatrix.setElement(i, 0, i);
        }
        for (int j = 0; j <= str2.length(); j++)
            distanceMatrix.setElement(0, j, j);
        int i = 1, j = 1;
        try {
            for (i = 1; i <= str1.length(); i++) {
                for (j = 1; j <= str2.length(); j++) {
                    distanceMatrix.setElement(i, j, Math.min(
                            Math.min(distanceMatrix.getElement(i - 1, j) + 1,
                                    distanceMatrix.getElement(i, j - 1) + 1),
                            distanceMatrix.getElement(i - 1, j - 1)
                                    + ((str1.charAt(i - 1) == str2
                                            .charAt(j - 1)) ? 0 : 1)));
                }
            }

        } catch (Throwable t) {
            logger.debug("[i, j]:" + "[" + i + "," + j + "]");
        }
        int distance = distanceMatrix.getElement(str1.length(), str2.length());
        // logger.debug("levenshtein distance for strings " + str1 + " and " +
        // str2 + "is: " + distance);
        return distance;
    }

    /**
     * Optimize method, to avoid java heap space, Based on LevenshteinDistance
     * algorithm
     * 
     * @param firstString
     * @param secondString
     * @return
     */
    private int xlevenshteinDistance(String str1, String str2) {
        // logger.debug("computing levenshtein distance for strings " + str1 +
        // " and " + str2);

        // this will further reduce the space by using shorter for array.
        if (str2.length() > str1.length()) {
            String temp = str1;
            str1 = str2;
            str2 = temp;
        }

        int[] row0Distance = new int[str2.length() + 1];
        int[] row1Distance = new int[str2.length() + 1];

        int rowId = 0, columnId = 0;

        for (columnId = 0; columnId <= str2.length(); columnId++)
            row0Distance[columnId] = columnId;

        for (rowId = 1; rowId <= str1.length(); rowId++) {
            for (columnId = 0; columnId <= str2.length(); columnId++) {
                if (columnId == 0) {
                    row1Distance[columnId] = rowId;
                } else {
                    row1Distance[columnId] = Math.min(
                            Math.min(row0Distance[columnId] + 1,
                                    row1Distance[columnId - 1] + 1),
                            row0Distance[columnId - 1]
                                    + ((str1.charAt(rowId - 1) == str2
                                            .charAt(columnId - 1)) ? 0 : 1));
                }
            }
            // discard row0, and make row1 as as row0 for next loop
            row0Distance = row1Distance;
            row1Distance = new int[str2.length() + 1];
        }

        int distance = row0Distance[str2.length()];
        // logger.debug("levenshtein distance for strings " + str1 + " and " +
        // str2 + "is: " + distance);
        return distance;
    }

}
