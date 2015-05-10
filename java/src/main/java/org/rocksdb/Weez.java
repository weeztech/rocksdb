package org.rocksdb;

public final class Weez {
    private Weez() {
    }

    private static native boolean dbGet0(
            long handle, long readOptHandle, long cfHandle, long key, int keyLen, long valueResultPtr) throws RocksDBException;


    public static native void freeBufResult(long handle, int size);


    public static boolean dbGet(RocksDB db, final ColumnFamilyHandle columnFamilyHandle, final ReadOptions opt, final long key, final int keyLen, long valueResultPtr) throws RocksDBException {
        return dbGet0(db.nativeHandle_, opt.nativeHandle_, columnFamilyHandle.nativeHandle_, key, keyLen, valueResultPtr);
    }


    private static native void riValue0(long handle, long valueResult) throws RocksDBException;

    public static void riValue(RocksIterator ri, long valueResult) throws RocksDBException {
        riValue0(ri.nativeHandle_, valueResult);
    }

    private static native boolean riStep0(long handle, int step, long untilKey, int untilKeyLen, boolean excludeUntilKey,
                                            long keyResult, long valueResult) throws RocksDBException;

    public static boolean riStep(RocksIterator ri,int step, long untilKey, int untilKeyLen, boolean excludeUntilKey,
                                  long keyResult, long valueResult) throws RocksDBException {
        return riStep0(ri.nativeHandle_, step, untilKey, untilKeyLen, excludeUntilKey, keyResult, valueResult);
    }

    private static native boolean riSeek0(long handle, long keyMin, int keyMinLen, boolean excludeMin,
                                    long keyMax, int keyMaxLen, boolean excludeMax,
                                    boolean backward,
                                    long keyResult, long valueResult) throws RocksDBException;

    public static boolean riSeek(RocksIterator ri,long keyMin, int keyMinLen, boolean excludeMin,
                                  long keyMax, int keyMaxLen, boolean excludeMax,
                                  boolean backward,
                                  long keyResult, long valueResult) throws RocksDBException {
        return riSeek0(ri.nativeHandle_, keyMin, keyMinLen, excludeMin, keyMax, keyMaxLen, excludeMax, backward, keyResult, valueResult);
    }

    private static native int wbPutAll0(long handle,long cfHandle, long block, long first);

    public static int wbPutAll(WriteBatch wb, ColumnFamilyHandle cfh, long block, long first) {
        return wbPutAll0(wb.nativeHandle_, cfh.nativeHandle_, block, first);
    }

    private static native long newWeezSumOperatorHandle();

    public static MergeOperator weezSumOperator = new MergeOperator() {
        @Override
        public long newMergeOperatorHandle() {
            return newWeezSumOperatorHandle();
        }
    };
}