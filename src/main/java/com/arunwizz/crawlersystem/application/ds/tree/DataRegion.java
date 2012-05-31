package com.arunwizz.crawlersystem.application.ds.tree;

public class DataRegion {
    int p;/*pre-order position of start child*/
    int i;/*the sibling relative location of the start child of the data region*/
    int j;/*the number of nodes in combination*/
    int k;/*the number of such tag nodes for all generalized nodes in a region*/
    
    public DataRegion(int p, int i, int j, int k) {
        this.p = p;
        this.i = i;
        this.j = j;
        this.k = k;
    }
    
    public void setRegionStartPreOrderPosition(int p) {
        this.p = p;
    }
    
    public void setRegionStartRelativePosition(int i) {
        this.i = i;
    }
    
    public void setNodeComb(int j) {
        this.j = j;
    }
    
    public void setNodeCount(int k) {
        this.k = k;
    }

    public int getRegionStartPreOrderPosition() {
        return p;
    }
    
    public int getRegionStartRelativePosition() {
        return i;
    }

    public int getNodeComb() {
        return j;
    }
    
    public int getNodeCount() {
        return k;
    }
    
    @Override
    public String toString(){
        return "[" + p + "," + i + "," + j + "," + k + "]";
    }
    
    @Override
    public boolean equals(Object dr) {
        return dr instanceof DataRegion
                && (this.getRegionStartPreOrderPosition() == ((DataRegion) dr).getRegionStartPreOrderPosition()
                        && this.getRegionStartRelativePosition() == ((DataRegion) dr)
                                .getRegionStartRelativePosition()
                        && this.getNodeCount() == ((DataRegion) dr)
                                .getNodeCount() && this.getNodeComb() == ((DataRegion) dr)
                        .getNodeComb());
    }
    
    @Override
    public int hashCode() {
        return 7*p + 17*i + 29*j + 31*k;//TODO: make a better hash
    }
}