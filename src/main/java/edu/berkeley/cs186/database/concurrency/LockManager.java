package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 * <p>
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 * <p>
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 * <p>
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 * <p>
 * This does mean that in the case of:
 * queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods you should implement!
        // Make sure to use these helper methods to abstract your code and
        // avoid re-implementing every time!

        /**
         * Check if a LOCKTYPE lock is compatible with preexisting locks.
         * Allows conflicts for locks held by transaction id EXCEPT.
         */
        boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            synchronized (this) {
                for (Lock curr : locks) {
                    if (!LockType.compatible(curr.lockType, lockType) && curr.transactionNum != except) {
                        return false;
                    }
                }
                return true;
            }
        }

        /**
         * Gives the transaction the lock LOCK. Assumes that the lock is compatible.
         * Updates lock on resource if the transaction already has a lock.
         */
        void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            synchronized (this) {
                long curr = lock.transactionNum;
                List<Lock> holdlocks = transactionLocks.get(curr);
                if (holdlocks == null) {
                    transactionLocks.put(curr, new ArrayList<>());
                }
                holdlocks = transactionLocks.get(curr);
                boolean substitutable = false;
                Lock old = null;
                for (Lock currLock : holdlocks) {
                    if (currLock.name == lock.name && LockType.substitutable(lock.lockType, currLock.lockType)) {
                        substitutable = true;
                        old = currLock;
                    }
                }
                int index1 = -1;
                int index2 = -1;
                if (substitutable) {
                    index1 = holdlocks.indexOf(old);
                    holdlocks.remove(old);
                    index2 = locks.indexOf(old);
                    locks.remove(old);
                }
                if(index1 != -1){
                    holdlocks.add(index1, lock);
                    locks.add(index2, lock);
                }
                else {
                    holdlocks.add(lock);
                    locks.add(lock);
                }
                return;
            }
        }

        /**
         * Releases the lock LOCK and processes the queue. Assumes it had been granted before.
         */
        void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            synchronized (this) {
                locks.remove(lock);
                transactionLocks.get(lock.transactionNum).remove(lock);
                processQueue();
                return;
            }
        }

        /**
         * Adds a request for LOCK by the transaction to the queue and puts the transaction
         * in a blocked state.
         */
        void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            synchronized (this) {
                if (addFront) {
                    waitingQueue.addFirst(request);
                } else {
                    waitingQueue.addLast(request);
                }
            }
            return;
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted.
         */
        private void processQueue() {
            // TODO(proj4_part1): implement
            synchronized (this) {
                while (!waitingQueue.isEmpty()) {
                    LockRequest curr = waitingQueue.peekFirst();
                    if (checkCompatible(curr.lock.lockType, curr.lock.transactionNum)) {
                        grantOrUpdateLock(curr.lock);
                        waitingQueue.pollFirst();
                        curr.transaction.unblock();
                    } else {
                        break;
                    }
                }
                return;
            }
        }

        /**
         * Gets the type of lock TRANSACTION has on this resource.
         */
        LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            synchronized (this) {
                List<Lock> holdlocks = transactionLocks.get(transaction);
                for (Lock curr : holdlocks) {
                    if (resourceEntries.get(curr.name).equals(this)) {
                        return curr.lockType;
                    }
                }
                return LockType.NL;
            }
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(proj4_part1): You may add helper methods here if you wish

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     * <p>
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     * <p>
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     * <p>
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     *                                       isn't being released
     * @throws NoLockHeldException           if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        ResourceEntry resource = getResourceEntry(name);
        synchronized (this) {
            Lock requirelock = new Lock(name, lockType, transaction.getTransNum());
            if (transactionLocks.get(transaction.getTransNum()) == null) {
                transactionLocks.put(transaction.getTransNum(), new ArrayList<>());
            }
            for (Lock curr : transactionLocks.get(transaction.getTransNum())) {
                if (curr.equals(requirelock)) {
                    throw new DuplicateLockRequestException("Duplicated Lock");
                }
            }

            for (ResourceName curr : releaseLocks) {
                boolean find = false;
                for (Lock lock : transactionLocks.get(transaction.getTransNum())) {
                    if (lock.name == curr) {
                        find = true;
                    }
                }
                if (!find) {
                    throw new NoLockHeldException("No Hold Lock");
                }
            }
            if (!resource.checkCompatible(lockType, transaction.getTransNum())) {
                transaction.prepareBlock();
                shouldBlock = true;
                LockRequest temp = new LockRequest(transaction, requirelock);
                resource.addToQueue(temp, true);
            } else {
                resource.grantOrUpdateLock(requirelock);
                for (ResourceName curr : releaseLocks) {
                    ResourceEntry entry = resourceEntries.get(curr);
                    List<Lock> needRelease = new ArrayList<>();
                    for (Lock lock : entry.locks) {
                        if (lock.transactionNum == transaction.getTransNum() && !requirelock.equals(lock)) {
                            needRelease.add(lock);
                        }
                    }
                    for (Lock currlock : needRelease) {
                        entry.releaseLock(currlock);
                    }
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
        return;
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     * <p>
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     *                                       TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        ResourceEntry resource = getResourceEntry(name);
        synchronized (this) {
            Lock requirelock = new Lock(name, lockType, transaction.getTransNum());
            if (transactionLocks.get(transaction.getTransNum()) == null) {
                transactionLocks.put(transaction.getTransNum(), new ArrayList<>());
            }
            for (Lock curr : transactionLocks.get(transaction.getTransNum())) {
                if (curr.equals(requirelock)) {
                    throw new DuplicateLockRequestException("Duplicated Lock");
                }
            }
            if (!resource.checkCompatible(lockType, transaction.getTransNum()) || !resource.waitingQueue.isEmpty()) {
                transaction.prepareBlock();
                shouldBlock = true;
                LockRequest temp = new LockRequest(transaction, requirelock);
                resource.addToQueue(temp, false);
            } else {
                resource.grantOrUpdateLock(requirelock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
        return;

    }

    /**
     * Release TRANSACTION's lock on NAME.
     * <p>
     * Error checking must be done before the lock is released.
     * <p>
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        ResourceEntry resource = getResourceEntry(name);
        synchronized (this) {
            if (transactionLocks.get(transaction.getTransNum()) == null) {
                throw new NoLockHeldException("No Hold Lock");
            }
            ResourceName curr = name;
            boolean find = false;
            for (Lock lock : transactionLocks.get(transaction.getTransNum())) {
                if (lock.name.toString().equals(curr.toString())) {
                    find = true;
                }
            }
            if (!find) {
                throw new NoLockHeldException("No Hold Lock");
            }
            List<Lock> needRelease = new ArrayList<>();
            for (Lock lock : resource.locks) {
                if (lock.transactionNum == transaction.getTransNum()) {
                    needRelease.add(lock);
                }
            }
            for(Lock currLock : needRelease){
                resource.releaseLock(currLock);
            }

            return;
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     * <p>
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     * <p>
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     *                                       NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException           if TRANSACTION has no lock on NAME
     * @throws InvalidLockException          if the requested lock type is not a promotion. A promotion
     *                                       from lock type A to lock type B is valid if and only if B is substitutable
     *                                       for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        ResourceEntry resource = getResourceEntry(name);
        synchronized (this) {
            Lock requirelock = new Lock(name, newLockType, transaction.getTransNum());
            if (transactionLocks.get(transaction.getTransNum()) == null) {
                throw new NoLockHeldException("NO Lock");
            }
            for (Lock curr : transactionLocks.get(transaction.getTransNum())) {
                if (curr.equals(requirelock)) {
                    throw new DuplicateLockRequestException("Duplicated Lock");
                }
            }
            boolean find = false;
            Lock old = null;
            for (Lock lock : transactionLocks.get(transaction.getTransNum())) {
                if (lock.name == name) {
                    find = true;
                    old = lock;
                }
            }
            if (!find) {
                throw new NoLockHeldException("No Hold Lock");
            }

            if(old.equals(requirelock) || !LockType.substitutable(newLockType, old.lockType)){
                throw new InvalidLockException("Invalid new lock");
            }

            if(newLockType == LockType.SIX){
                acquireAndRelease(transaction, name, newLockType, new ArrayList<ResourceName>());
                return;
            }
            else{
                if(!resource.checkCompatible(newLockType, transaction.getTransNum())){
                    LockRequest request = new LockRequest(transaction, requirelock);
                    resource.addToQueue(request, true);
                    transaction.prepareBlock();
                    shouldBlock = true;
                }
                else {
                    resource.grantOrUpdateLock(requirelock);
                }
            }


        }
        if(shouldBlock){
            transaction.block();
        }
        return;
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry curr = getResourceEntry(name);
        LockType result = LockType.NL;
        for (Lock lock : curr.locks) {
            if (lock.transactionNum == transaction.getTransNum()) {
                result = lock.lockType;
            }
        }

        return result;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
