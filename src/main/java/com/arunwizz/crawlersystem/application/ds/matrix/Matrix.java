package com.arunwizz.crawlersystem.application.ds.matrix;

import java.util.ArrayList;
import java.util.Collection;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.MaximizeAction;

public class Matrix<T> {
   
    private ArrayList<ArrayList<T>> matrix;
    private int rowSize;
    private int colSize;
    
    
    /**
     * Matrix constructor
     * @param m
     * @param n
     */
    public Matrix(int m, int n) {
        /*Initialize the matrix arraylist*/
        this.matrix = new ArrayList<ArrayList<T>>(m);
        for (int rowIndex = 0; rowIndex < m; rowIndex++) {
            ArrayList<T> tempList = new ArrayList<T>(n);
            for (int colIndex = 0; colIndex < n; colIndex++) {
                tempList.add(null);
            }
            this.matrix.add(tempList);
        }
        rowSize = m;
        colSize = n;
    }
    
    public T getElement(int i, int j) throws MatrixException {
        if (i < 0 || i > rowSize -1 || j < 0 || j > colSize - 1) {
            throw new MatrixException("Matrix index out of bound: " + "(" + i + ", " + j + ")");
        } else {
            return matrix.get(i).get(j);
        }
     }

    public void setElement(int i, int j, T element) throws MatrixException {
        if (i < 0 || i > rowSize -1 || j < 0 || j > colSize - 1) {
            throw new MatrixException("Matrix index out of bound: " + "(" + i + ", " + j + ")");
        } else {
            matrix.get(i).set(j, element);
        }
    }
    
    public int getRowSize() {
        return rowSize;
    }
    
    public int getColSize() {
        return colSize;
    }
    
    public ArrayList<T> getRow(int rowId) {
        if (rowId < 0 || rowId > rowSize - 1) {
            throw new MatrixException("Matrix row index out of bound: " + "(" + rowId + ")");
        }
        return matrix.get(rowId);
    }
    
    public ArrayList<T> getColumn(int colId) {
        if (colId < 0 || colId > colSize - 1) {
            throw new MatrixException("Matrix column index out of bound: " + "(" + colId + ")");
        }
        ArrayList<T> columnValue = new ArrayList<T>(rowSize);
        for (int row = 0; row < rowSize; row++) {
            columnValue.add(this.getElement(row, colId));
        }
        return columnValue;
    }    
    
    @Override
    public String toString(){
       
        int maxWidth = 0;
        try {
            //Determine the integer with max number of literals/ width
            for (int k = 0; k < getRowSize()*getColSize(); k++) {
                int i = k / getColSize();
                int j = k % getColSize();
                T ij = getElement(i, j);
                if (String.valueOf(ij).length() > maxWidth) {
                    maxWidth = String.valueOf(ij).length();
                }
            }
            //System.out.println("max width: " + maxWidth);
        } catch (MatrixException me) {
            //TODO: just log and use no formating
            maxWidth = 0;
        }
        
       
        StringBuilder sbuilder = new StringBuilder("|");
        for (int i = 1; i <= getColSize(); i++) {
            sbuilder.append("%" + i + "$" + (maxWidth+1) + "s ");
        }
        sbuilder.append("|").append(System.getProperty("line.separator"));
        String format = sbuilder.toString();

        //Reset the string builder, and initialize with matrix dimension
        sbuilder = new StringBuilder(getRowSize() + " X " + getColSize() + System.getProperty("line.separator") + "-------" + System.getProperty("line.separator"));
        
        for (ArrayList<T> row: matrix) {
            String[] rowString = new String[getColSize()];
            int i = 0;
            for (T column: row) {
                rowString[i++] = String.valueOf(column);
            }
            sbuilder.append(String.format(format, (Object[])rowString));
        }
        return sbuilder.toString();
    }


    
}
