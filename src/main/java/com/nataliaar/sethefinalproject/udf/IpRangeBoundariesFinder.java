package com.nataliaar.sethefinalproject.udf;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;

public class IpRangeBoundariesFinder extends UDF {

    public List<LongWritable> evaluate(String str) {
        if (str == null) {
            return null;
        }

        return getIpRangeBoundaries(str);
    }

    private List<LongWritable> getIpRangeBoundaries(String inputParameter) {
        SubnetInfo subnetInfo = new SubnetUtils(inputParameter).getInfo();
        String lowAddress = subnetInfo.getLowAddress();
        String highAddress = subnetInfo.getHighAddress();

        List<LongWritable> ipRangeBoundaries = new ArrayList<LongWritable>();
        ipRangeBoundaries.add(new LongWritable(UdfUtils.convertIpToDecimal(lowAddress)));
        ipRangeBoundaries.add(new LongWritable(UdfUtils.convertIpToDecimal(highAddress)));

        return ipRangeBoundaries;
    }
}
