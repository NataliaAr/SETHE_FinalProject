package com.nataliaar.sethefinalproject.udf;

public final class UdfUtils {

    public static long convertIpToDecimal(String ipAddress) {
        String[] addrArray = ipAddress.split("\\.");

        long ipDecimal = 0;

        for (int i = 0; i < addrArray.length; i++) {

            int power = 3 - i;
            ipDecimal += ((Integer.parseInt(addrArray[i]) % 256 * Math.pow(256, power)));
        }

        return ipDecimal;
    }
    
}