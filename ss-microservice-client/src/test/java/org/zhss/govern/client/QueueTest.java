package org.zhss.govern.client;

import java.util.concurrent.LinkedBlockingQueue;

public class QueueTest {

    public static void main(String[] args) throws Exception {
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
        queue.offer("zhangsan");
        queue.offer("lisi");
        queue.offer("wangwu");

        System.out.println(queue);

        String element = queue.peek();
        System.out.println(element);
        System.out.println(queue);

        element = queue.poll();
        System.out.println(element);
        System.out.println(queue);
    }

}
