package com.arunwizz.crawlersystem.application.autometa;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class NFA {
    
    private int stateCounter = 0;
   
    private Set<TransitionRelation> transitionMap = new HashSet<TransitionRelation>(); 
    
    private static class TransitionRelation {
        
        public TransitionRelation(State state, Symbol symbol){
            currentState = state;
            this.symbol = symbol;
        }

        public TransitionRelation(State state, Symbol symbol, State newState){
            currentState = state;
            this.symbol = symbol;
            this.newState = newState;
        }
        
        private State currentState;
        private Symbol symbol;
        private State newState;
        
        @Override
        public int hashCode(){
            return this.currentState.hashCode()+this.symbol.hashCode();
        }
        
        @Override
        public boolean equals(Object transitionRelation){
            if (transitionRelation instanceof TransitionRelation) {
                if (this.currentState.equals(((TransitionRelation) transitionRelation).currentState) &&
                this.symbol.equals(((TransitionRelation) transitionRelation).symbol)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
    
    private static class State {
        public State(int label) {
            this.stateLabel = label;
        }
        private int stateLabel;
        
        @Override
        public boolean equals(Object state) {
            if (state instanceof State) {
                if (this.stateLabel ==((State)state).stateLabel){
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        
        @Override
        public int hashCode(){
            return stateLabel;
        }        
    }
    
    private static class Symbol {
        public Symbol(String label) {
            this.symbolLabel = label;
        }
        private String symbolLabel;
        
        @Override
        public boolean equals(Object symbol) {
            if (symbol instanceof Symbol) {
                if (this.symbolLabel.equals(((Symbol)symbol).symbolLabel)){
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        
        @Override
        public int hashCode(){
            return symbolLabel.hashCode();
        }
    }
    
    
    private Symbol epsilon = new Symbol("epsilon");
    private Set<State> states = null;
    private Set<Symbol> symbols = null;
    private Set<TransitionRelation> transitionStates = null;
    private State startState = null;
    private Set<State> acceptStates = null;
    
    private List<String> inputSymbols = null;
    
    public NFA(List<String> symbolList) {
        inputSymbols = symbolList;
        states = new HashSet<NFA.State>();
        for (String symbol:symbolList) {
            symbols.add(new Symbol(symbol));
        }
        transitionStates = new HashSet<NFA.TransitionRelation>();
        startState = new State(stateCounter);
        acceptStates = new HashSet<NFA.State>();
    }
    
    private TransitionRelation getTransitionRelation(TransitionRelation tr) {
        Iterator<TransitionRelation> iterator = transitionStates.iterator();

        while (iterator.hasNext()) {
            TransitionRelation tempTr = iterator.next();
            if (tempTr.equals(tr)) {
                return tempTr;
            }
        }
        return null;
    }
    
    
    private TransitionRelation getTransitionRelation(Symbol s) {
        for (TransitionRelation tr: transitionStates) {
            if(tr.symbol.equals(s)){
                return tr;
            }
        }
        return null;
    }

    private TransitionRelation getTransitionRelation(Symbol s, State nextState) {
        //there exists qf,s to qi for f>=c
        for(TransitionRelation tr:transitionStates) {
            if (tr.symbol.equals(s) && tr.newState.equals(nextState) && tr.currentState.stateLabel >= qc.stateLabel){
                return tr;
            }
        }
        return null;
    }
    
    private TransitionRelation getTransitionRelation(State currentState, Symbol s) {
        for(TransitionRelation tr:transitionStates) {
            if(tr.currentState.equals(currentState) && tr.symbol.equals(s) && tr.currentState.stateLabel > currentState.stateLabel){
                return tr;
            }
        }
        return null;
    }
    
    
    private State qc = startState;
    public String generateGrammar() {
        for (int i = 0; i < inputSymbols.size(); i++){
            String currentSymbolString = inputSymbols.get(i);
            //check if there exists a transition
            Symbol currentSymbol = new Symbol(currentSymbolString);
            TransitionRelation tr = null;
            if((tr = getTransitionRelation(new TransitionRelation(qc, currentSymbol)))!= null){//4
                qc = tr.newState;
            } else if ((tr=getTransitionRelation(currentSymbol)) != null) {//6
                State qi = tr.currentState;
                State qj = tr.newState;
                if ((tr=getTransitionRelation(epsilon, qi))!=null) {//7
                    State qf = tr.currentState;
                    transitTo(qc, qf);
                } else {
                    transitTo(qc, qi);
                }
                qc = qj;
            } else {//11
                State qc1 = new State(++stateCounter);
                states.add(qc1);
                transitionStates.add(new TransitionRelation(qc, epsilon, qc1));
                qc = qc1;
            }
            if (i == inputSymbols.size()-1){//14
                State qr = getState(stateCounter);
                acceptStates.add(qr);
                transitTo(qc, qr);
            }
        }
        
        return generateRegExpr();
    }

    private String generateRegExpr() {
        // TODO Auto-generated method stub
        return null;
        
    }

    private void transitTo(State qc, State qf) {
        while (!qc.equals(qf)) {
            TransitionRelation tr = null;
            if ((tr=getTransitionRelation(qc, epsilon))!=null){//2
                State qk = tr.newState;
                qc = qk;
            } else {
                State qc1 = getState(qc.stateLabel+1);
                transitionStates.add(new TransitionRelation(qc, epsilon, qc1));
                qc=qc1;
            }
        }
        
    }

    private State getState(int i) {
        Iterator<State> it = states.iterator();
        while(it.hasNext()) {
            State st = it.next();
            if (st.stateLabel == i) {
                return st;
            }
        }
        return null;
    }
    

}
