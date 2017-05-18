package io.snappydata;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class SQLServerCdcBase extends JavaCdcStreamingBase {

  public SQLServerCdcBase(String[] args, Map<String, String> extraProps) throws Exception {
    super(args, extraConfigProps(extraProps));
  }

  private static Map<String, String> extraConfigProps(Map<String, String> extraProps) {
    if(extraProps == null) {
      extraProps = new LinkedHashMap<>();
    }
    extraProps.putIfAbsent("jdbc.offsetColumn","__$start_lsn");
    extraProps.putIfAbsent("jdbc.offsetToStrFunc","master.dbo.fn_varbintohexstr");
    extraProps.putIfAbsent("jdbc.offsetIncFunc", "master.dbo.fn_cdc_increment_lsn");
    extraProps.putIfAbsent("jdbc.strToOffsetFunc","master.dbo.fn_cdc_hexstrtobin");
    extraProps.putIfAbsent("jdbc.getNextOffset",
        // make sure to convert the LSN to string and give a column alias for the
        // user defined query one is supplying. Alternatively, a procedure can be invoked
        // with $table, $currentOffset Snappy provided variables returning one string column
        // representing the next LSN string.
        "select master.dbo.fn_varbintohexstr(max(__$start_lsn)) nextLSN from (" +
        "  select __$start_lsn, sum(count(1)) over (order by __$start_lsn) runningCount" +
        "  from $table where __$start_lsn > master.dbo.fn_cdc_hexstrtobin('$currentOffset')" +
        "  group by __$start_lsn" +
        ") x where runningCount <= $maxEvents");
    return extraProps;
  }
}

