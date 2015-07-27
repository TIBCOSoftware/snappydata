package org.apache.spark.sql.store;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.compile.FromList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.SubqueryList;
import com.pivotal.gemfirexd.internal.shared.common.SingleHopInformation;
import org.apache.spark.sql.collection.UUIDRegionKey;

import java.io.Serializable;
import java.util.Set;
import java.util.Vector;

/**
 * Created by soubhikc on 21/7/15.
 */
public class CachedBatchPartitioner extends GfxdPartitionResolver {

    public final static String resolver = CachedBatchPartitioner.class.getName();

    public CachedBatchPartitioner() {
        super();
        this.isPrimaryKeyPartitioningKey = true;
    }

    @Override
    public void bindExpression(FromList fromList, SubqueryList subqueryList, Vector<?> aggregateVector) throws StandardException {

    }

    @Override
    public Object getRoutingObject(Object key, Object val, Region<?, ?> region) {
        if (key instanceof String) {
            return UUIDRegionKey.parseAndGetBucketId((String) key);
        }
        return key;
    }

    @Override
    protected boolean isPartitioningSubsetOfKey() {
        return false;
    }

    @Override
    public Object getRoutingKeyForColumn(DataValueDescriptor partitionColumnValue) {
        return null;
    }

    @Override
    public Object[] getRoutingObjectsForRange(DataValueDescriptor lowerBound, boolean lowerBoundInclusive, DataValueDescriptor upperBound, boolean upperBoundInclusive) {
        return new Object[0];
    }

    @Override
    public String getDDLString() {
        return "PARTITION BY RESOLVER '" + resolver + "'";
    }

    @Override
    public Serializable getRoutingObjectFromDvdArray(DataValueDescriptor[] values) {
        return null;
    }

    @Override
    public Object[] getRoutingObjectsForList(DataValueDescriptor[] keys) {
        return new Object[0];
    }

    @Override
    public Object getRoutingObjectsForPartitioningColumns(DataValueDescriptor[] partitioningColumnObjects) {
        return null;
    }

    @Override
    public String[] getColumnNames() {
        return new String[0];
    }

    @Override
    public void setColumnInfo(TableDescriptor td, Activation activation) throws StandardException {

    }

    @Override
    public GfxdPartitionResolver cloneObject() {
        return null;
    }

    @Override
    public boolean okForColocation(GfxdPartitionResolver rslvr) {
        return false;
    }

    @Override
    protected void setPartitionColumns(String[] partCols, String[] refPartCols) {

    }

    @Override
    public boolean isUsedInPartitioning(String columnToCheck) {
        return false;
    }

    @Override
    public int getPartitioningColumnIndex(String partitionColumn) {
        return 0;
    }

    @Override
    public boolean requiresGlobalIndex() {
        return false;
    }

    @Override
    public void setResolverInfoInSingleHopInfoObject(SingleHopInformation info) throws StandardException {

    }

    @Override
    public String getName() {
        return "snappy-spark-cached-batch-partition-resolver";
    }

    public String toString() {
        return resolver;
    }
}