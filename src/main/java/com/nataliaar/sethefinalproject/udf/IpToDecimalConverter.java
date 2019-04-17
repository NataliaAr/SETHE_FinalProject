package com.nataliaar.sethefinalproject.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;

public class IpToDecimalConverter extends UDF {

    private LongWritable result = new LongWritable();

    public LongWritable evaluate(String str) {
        if (str == null) {
            return null;
        }
        result.set(UdfUtils.convertIpToDecimal(str));
        return result;
    }
}
