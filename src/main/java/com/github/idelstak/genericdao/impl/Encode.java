/*
 * Copyright (c) 2012-2016 Jeffrey L. Eppinger.  All Rights Reserved.
 *     You may use, modify and share this code for non-commercial purposes
 *     as long a you comply with this license from Creative Commons:
 *     Summary of license: http://creativecommons.org/licenses/by-nc-sa/3.0
 *     Full Text of License: http://creativecommons.org/licenses/by-nc-sa/3.0/legalcode
 *     Specifically, if you distribute your code for non-educational purposes,
 *     you must include this copyright notice in your work.
 *     If you wish to have broader rights, you must contact the copyright holder.
 */
package com.github.idelstak.genericdao.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Encode {

    private Encode() {
    }
    private static final int NULL_CODE = -10;
    private static final int BOOLEAN_CODE = -11;
    private static final int DATE_CODE = -12;
    private static final int DOUBLE_CODE = -13;
    private static final int FLOAT_CODE = -14;
    private static final int INT_CODE = -15;
    private static final int LONG_CODE = -16;

    private static void encodeInt(byte[] a, int code) {
        a[0] = (byte) (code >> 24);
        a[1] = (byte) (code >> 16);
        a[2] = (byte) (code >> 8);
        a[3] = (byte) (code);
    }

    private static byte[] getBooleanBytes(Boolean xObj) {
        byte[] a = new byte[5];
        encodeInt(a, BOOLEAN_CODE);

        boolean x = xObj;
        if (x) {
            a[4] = 1;
        } else {
            a[4] = 0;
        }

        return a;
    }

    public static byte[] getBytes(Object obj) {
        if (obj == null) {
            return Encode.getNullBytes();
        }
        if (obj instanceof Boolean) {
            return Encode.getBooleanBytes((Boolean) obj);
        }
        if (obj instanceof Double) {
            return Encode.getDoubleBytes((Double) obj);
        }
        if (obj instanceof Float) {
            return Encode.getFloatBytes((Float) obj);
        }
        if (obj instanceof Integer) {
            return Encode.getIntBytes((Integer) obj);
        }
        if (obj instanceof Long) {
            return Encode.getLongBytes((Long) obj);
        }

        if (obj instanceof java.util.Date) {
            return Encode.getDateBytes((java.util.Date) obj);
        }
        if (obj instanceof String) {
            return Encode.getStringBytes((String) obj);
        }
        if (obj instanceof byte[]) {
            return Encode.getBytesBytes((byte[]) obj);
        }

        if (obj instanceof Object[]) {
            try {
                Object[] dbValueArray = (Object[]) obj;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                baos.write(Encode.getRawIntBytes(dbValueArray.length));
                for (int i = 0; i < dbValueArray.length; i++) {
                    baos.write(getBytes(dbValueArray[i]));
                }
                return baos.toByteArray();
            } catch (IOException e) {
                // Can't happen
                e.printStackTrace();
                throw new AssertionError("IOException thrown from ByteArrayOutputStream.write(byte[]): " + e.getMessage());
            }
        }

        throw new AssertionError("Unknown object type: " + obj.getClass().getName());
    }

    private static byte[] getBytesBytes(byte[] inputBytes) {
        int len = inputBytes.length;
        byte[] answer = new byte[len + 4];
        encodeInt(answer, len);  // The code for arrays is just a positive length

        for (int i = 0; i < len; i++) {
            answer[i + 4] = inputBytes[i];
        }
        return answer;

    }

    private static byte[] getDateBytes(java.util.Date date) {
        byte[] a = new byte[12];
        encodeInt(a, DATE_CODE);

        long bits = date.getTime();
        a[4] = (byte) (bits >> 56);
        a[5] = (byte) (bits >> 48);
        a[6] = (byte) (bits >> 40);
        a[7] = (byte) (bits >> 32);
        a[8] = (byte) (bits >> 24);
        a[9] = (byte) (bits >> 16);
        a[10] = (byte) (bits >> 8);
        a[11] = (byte) (bits);
        return a;
    }

    private static byte[] getDoubleBytes(double x) {
        byte[] a = new byte[8];
        encodeInt(a, DOUBLE_CODE);

        long bits = Double.doubleToLongBits(x);
        a[4] = (byte) (bits >> 56);
        a[5] = (byte) (bits >> 48);
        a[6] = (byte) (bits >> 40);
        a[7] = (byte) (bits >> 32);
        a[8] = (byte) (bits >> 24);
        a[9] = (byte) (bits >> 16);
        a[10] = (byte) (bits >> 8);
        a[11] = (byte) (bits);
        return a;
    }

    private static byte[] getFloatBytes(float x) {
        byte[] a = new byte[8];
        encodeInt(a, FLOAT_CODE);

        int bits = Float.floatToRawIntBits(x);
        a[4] = (byte) (bits >> 24);
        a[5] = (byte) (bits >> 16);
        a[6] = (byte) (bits >> 8);
        a[7] = (byte) (bits);
        return a;
    }

    private static byte[] getIntBytes(int x) {
        byte[] a = new byte[8];
        encodeInt(a, INT_CODE);

        a[4] = (byte) (x >> 24);
        a[5] = (byte) (x >> 16);
        a[6] = (byte) (x >> 8);
        a[7] = (byte) (x);
        return a;
    }

    private static byte[] getLongBytes(long x) {
        byte[] a = new byte[12];
        encodeInt(a, LONG_CODE);

        a[4] = (byte) (x >> 56);
        a[5] = (byte) (x >> 48);
        a[6] = (byte) (x >> 40);
        a[7] = (byte) (x >> 32);
        a[8] = (byte) (x >> 24);
        a[9] = (byte) (x >> 16);
        a[10] = (byte) (x >> 8);
        a[11] = (byte) (x);
        return a;
    }

    private static byte[] getNullBytes() {
        byte[] a = new byte[4];
        encodeInt(a, NULL_CODE);
        return a;
    }

    private static byte[] getStringBytes(String s) {
        byte[] sBytes = s.getBytes();
        byte[] a = new byte[sBytes.length + 4];
        encodeInt(a, sBytes.length);
        for (int i = 0; i < sBytes.length; i++) {
            a[i + 4] = sBytes[i];
        }
        return a;
    }

    private static byte[] getRawIntBytes(int x) {
        byte[] a = new byte[4];
        encodeInt(a, x);
        return a;
    }
}
