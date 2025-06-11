package com.zhss.microservice.server.node;

/**
 * server节点的角色
 */
public class ServerNodeRole {

    /**
     * 普通master节点
     */
    public static final int COMMON_NODE = 0;
    /**
     * Controller角色
     */
    public static final int CONTROLLER = 1;
    /**
     * Controller候选人角色
     */
    public static final int CANDIDATE = 2;

    private Integer role;

    private ServerNodeRole() {

    }

    static class Singleton {
        static ServerNodeRole instance = new ServerNodeRole();
    }

    public static ServerNodeRole getInstance() {
        return Singleton.instance;
    }

    public Integer getRole() {
        return role;
    }

    public static void setRole(Integer role) {
        getInstance().role = role;
    }

    public static Boolean isController() {
        return getInstance().role.equals(CONTROLLER);
    }

    public static Boolean isCandidate() {
        return getInstance().role.equals(CANDIDATE);
    }

}
