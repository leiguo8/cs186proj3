package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction

        if(transaction == null){
            return;
        }
        if(lockType == LockType.NL){
            return;
        }
        LockType currHold = lockContext.lockman.getLockType(transaction, lockContext.name);
        boolean[] justReturn = new boolean[1];
        justReturn[0] = false;
        if(currHold == lockType || LockType.substitutable(currHold, lockType)){
            return;
        }
        else{
            if(lockType == LockType.S){
                helperParentGetLock(transaction, lockContext, LockType.IS, justReturn);
                if(justReturn[0]){
                    return;
                }
                if(currHold == LockType.NL){
                    lockContext.acquire(transaction, lockType);
                }
                else{
                    if(currHold == LockType.IX){
                        List<ResourceName> needRelease = sisDescendants(transaction, lockContext);
                        needRelease.add(lockContext.name);
                        lockContext.lockman.acquireAndRelease(transaction, lockContext.name, LockType.SIX,
                                needRelease);
                    }
                    else{
                        lockContext.escalate(transaction);
                    }
                }

            }
            if(lockType == LockType.X){
                helperParentGetLock(transaction, lockContext, LockType.IX, justReturn);
                if(justReturn[0]){
                    return;
                }
                if(currHold == LockType.NL){
                    lockContext.acquire(transaction, lockType);
                }
                else{
                    lockContext.escalate(transaction);
                    lockContext.promote(transaction, lockType);
                }
            }
        }

        return;
    }

    // TODO(proj4_part2): add helper methods as you see fit

    private static void helperParentGetLock(TransactionContext transaction, LockContext lockContext, LockType lockType,
                                            boolean[] justReturn){
        LockContext parent = lockContext.parent;
        if(parent == null){
            return;
        }
        LockType parentLockType = parent.lockman.getLockType(transaction, parent.name);
        if(!LockType.substitutable(parentLockType, lockType)){
            helperParentGetLock(transaction, parent, lockType, justReturn);
            if(justReturn[0]){
                return;
            }
            if(parent.lockman.getLockType(transaction,parent.name) == LockType.NL) {
                parent.acquire(transaction, lockType);
            }else{
                parent.promote(transaction, lockType);
            }
        }
        else if (parentLockType != LockType.IS && parentLockType != LockType.IX){
            justReturn[0] = true;
        }
        return;
    }

    private static List<ResourceName> sisDescendants(TransactionContext transaction, LockContext lockContext) {
        // TODO(proj4_part2): implement
        List<ResourceName> result = new ArrayList<>();
        List<edu.berkeley.cs186.database.concurrency.Lock> alllocks = lockContext.lockman.getLocks(transaction);
        for(Lock lock : alllocks){
            if((lock.lockType == LockType.S || lock.lockType == LockType.IS) && lock.name.isDescendantOf(lockContext.name)){
                result.add(lock.name);
            }
        }
        return result;
    }
}
