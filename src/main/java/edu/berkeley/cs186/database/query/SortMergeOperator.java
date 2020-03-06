package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Table;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            SortOperator sortLeft = new SortOperator(getTransaction(),this.getLeftTableName(),new LeftRecordComparator());
            SortOperator sortRight = new SortOperator(getTransaction(), this.getRightTableName(), new RightRecordComparator());
            String left = sortLeft.sort();
            String right = sortRight.sort();
            leftIterator = SortMergeOperator.this.getRecordIterator(left);
            rightIterator = SortMergeOperator.this.getRecordIterator(right);
            marked = false;
            nextRecord = null;
            leftRecord = leftIterator.next();
            rightRecord = rightIterator.next();

        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement
            nextRecord = null;
            if(leftRecord == null){
                return false;
            }
            else{
                if(!marked){
                    while(leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                    rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex())) < 0){
                        if(!leftIterator.hasNext()){
                            return false;
                        }
                        leftRecord = leftIterator.next();
                    }
                    while(leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                            rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex())) > 0){
                        if(!rightIterator.hasNext()) {
                            return false;
                        }
                        rightRecord = rightIterator.next();
                    }
                    rightIterator.markPrev();
                    marked = true;
                }
                if(leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex())) == 0){
                    List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
                    List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                    leftValues.addAll(rightValues);
                    this.nextRecord = new Record(leftValues);
                    if(!rightIterator.hasNext()){
                        rightIterator.reset();
                        if(!leftIterator.hasNext()){
                            leftRecord = null;
                        }
                        else {
                            leftRecord = leftIterator.next();
                        }
                        marked = false;
                    }
                    else {
                        rightRecord = rightIterator.next();
                    }
                    return true;
                }
                else{
                    rightIterator.reset();
                    if(!leftIterator.hasNext()){
                        leftRecord = null;
                    }
                    else {
                        leftRecord = leftIterator.next();
                    }
                    marked = false;
                    return hasNext();
                }
            }
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement
            if(hasNext()){
                return nextRecord;
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
