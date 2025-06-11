package com.zhss.microservice.common.utils;

/**
 * 字符串工具类
 */
public class StringUtils {

    /**
     * 判断字符串是否为空
     * @param string 字符串
     * @return 是否为空
     */
    public static boolean isEmpty(String string) {
        if(string == null || "".equals(string)) {
            return true;
        }
        return false;
    }

    /**
     * 判断字符串是否不为空
     * @param string 字符串
     * @return 是否为空
     */
    public static boolean isNotEmpty(String string) {
        if(string != null && !"".equals(string)) {
            return true;
        }
        return false;
    }

}
