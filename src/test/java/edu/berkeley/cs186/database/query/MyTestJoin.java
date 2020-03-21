package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.Page;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.table.Record;

import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

@Category({Proj3Tests.class, Proj3Part1Tests.class})
public class MyTestJoin {
    private Database d;
    private long numIOs;
    private QueryOperator leftSourceOperator;
    private QueryOperator rightSourceOperator;
    private Map<Long, Page> pinnedPages = new HashMap<>();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        d = new Database(tempDir.getAbsolutePath(), 256);
        d.setWorkMem(5); // B=5
        d.waitAllTransactions();
    }

    @After
    public void cleanup() {
        for (Page p : pinnedPages.values()) {
            p.unpin();
        }
        d.close();
    }

    // 4 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            4000 * TimeoutScaling.factor)));

    private void startCountIOs() {
        d.getBufferManager().evictAll();
        numIOs = d.getBufferManager().getNumIOs();
    }

    private void checkIOs(String message, long minIOs, long maxIOs) {
        if (message == null) {
            message = "";
        } else {
            message = "(" + message + ")";
        }

        long newIOs = d.getBufferManager().getNumIOs();
        long IOs = newIOs - numIOs;

        assertTrue(IOs + " I/Os not between " + minIOs + " and " + maxIOs + message,
                minIOs <= IOs && IOs <= maxIOs);
        numIOs = newIOs;
    }

    private void checkIOs(String message, long numIOs) {
        checkIOs(message, numIOs, numIOs);
    }

    private void checkIOs(long minIOs, long maxIOs) {
        checkIOs(null, minIOs, maxIOs);
    }

    private void checkIOs(long numIOs) {
        checkIOs(null, numIOs, numIOs);
    }

    private void setSourceOperators(TestSourceOperator leftSourceOperator,
                                    TestSourceOperator rightSourceOperator, Transaction transaction) {
        setSourceOperators(
                new MaterializeOperator(leftSourceOperator, transaction.getTransactionContext()),
                new MaterializeOperator(rightSourceOperator, transaction.getTransactionContext())
        );
    }

    private void pinPage(int partNum, int pageNum) {
        long pnum = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        Page page = d.getBufferManager().fetchPage(new DummyLockContext(), pnum, false);
        this.pinnedPages.put(pnum, page);
    }

    private void unpinPage(int partNum, int pageNum) {
        long pnum = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        this.pinnedPages.remove(pnum).unpin();
    }

    private void evictPage(int partNum, int pageNum) {
        long pnum = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        this.d.getBufferManager().evict(pnum);
        numIOs = d.getBufferManager().getNumIOs();
    }

    private void setSourceOperators(QueryOperator leftSourceOperator,
                                    QueryOperator rightSourceOperator) {
        assert (this.leftSourceOperator == null && this.rightSourceOperator == null);

        this.leftSourceOperator = leftSourceOperator;
        this.rightSourceOperator = rightSourceOperator;

        // hard-coded mess, but works as long as the first two tables created are the source operators
        pinPage(1, 0); // information_schema.tables header page
        pinPage(1, 3); // information_schema.tables entry for left source
        pinPage(1, 4); // information_schema.tables entry for right source
        pinPage(3, 0); // left source header page
        pinPage(4, 0); // right source header page
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleJoinPNLJ() {
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    new TestSourceOperator(),
                    new TestSourceOperator(),
                    transaction
            );
            transaction.createTable(TestUtils.createSchemaWithAllTypes(),"empty");
            QueryOperator t = new SequentialScanOperator(transaction.getTransactionContext(), "empty");

            JoinOperator joinOperator = new SortMergeOperator(leftSourceOperator, t, "int", "int",
                    transaction.getTransactionContext());


            Iterator<Record> outputIterator = joinOperator.iterator();

            assertFalse(outputIterator.hasNext());

        }
    }
}
