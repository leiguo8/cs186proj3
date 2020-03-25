package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if(a == NL || b == NL){
            return true;
        }
        else if (a == IS){
            if( b == X){
                return false;
            }
            return true;
        }
        else if(a == IX){
            if(b == S || b == SIX || b == X){
                return false;
            }
            return true;
        }
        else if(a == S){
            if(b == IX || b == SIX || b == X){
                return false;
            }
            return true;
        }
        else if(a == SIX){
            if(b == NL || b == IS){
                return true;
            }
            return false;
        }
        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if(parentLockType == NL){
            if(childLockType == NL){
                return true;
            }
            return false;
        }
        else if(parentLockType == S){
            if(childLockType == NL){
                return true;
            }
            return false;
        }
        else if(parentLockType == X){
            if(childLockType != NL){
                return false;
            }
            return true;
        }
        else if(parentLockType == IS){
            if(childLockType == NL || childLockType == IS || childLockType == S){
                return true;
            }
            return false;
        }
        else if(parentLockType == IX){
            return true;
        }
        else{
            if(childLockType == IS || childLockType == S){
                return false;
            }
            return true;
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if(substitute == NL){
            if(required != NL){
                return false;
            }
            return true;
        }
        else if (substitute == S){
            if(required == S || required == NL || required == IS){
                return true;
            }
            return false;
        }
        else if(substitute == X){
            if(required == S || required == NL || required == X || required == IS || required == IX || required ==SIX){
                return true;
            }
            return false;
        }
        else if(substitute == IS){
            if(required == NL || required == IS){
                return true;
            }
            return false;
        }
        else if(substitute == IX){
            if(required == NL || required == IX || required == IS){
                return true;
            }
            return false;
        }
        else{
            if(required == NL || required == SIX || required == S || required == IX){
                return true;
            }
            return false;
        }

    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

