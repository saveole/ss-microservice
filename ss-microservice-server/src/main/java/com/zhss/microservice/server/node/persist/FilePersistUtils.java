package com.zhss.microservice.server.node.persist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zhss.microservice.server.config.Configuration;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class FilePersistUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePersistUtils.class);

    /**
     * 持久化槽位分配数据到本地磁盘
     */
    public static Boolean persist(byte[] bytes, String filename) {
        try {
            // 获取到数据存储目录
            Configuration configuration = Configuration.getInstance();
            File dataDir = new File(configuration.getDataDir());
            if(!dataDir.exists()) {
                dataDir.mkdirs();
            }

            // 获取针对磁盘文件的一系列的输出流
            File slotAllocationFile = new File(dataDir, filename);
            FileOutputStream fileOutputStream = new FileOutputStream(slotAllocationFile);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
            DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);

            // 在磁盘文件里写入一份checksum校验和
            Checksum checksum = new Adler32();
            checksum.update(bytes, 0, bytes.length);
            long checksumValue = checksum.getValue();
            dataOutputStream.writeLong(checksumValue);
            // 写入槽位分配数据的长度
            dataOutputStream.writeInt(bytes.length);
            // 写入槽位分配数据
            dataOutputStream.write(bytes);

            // 对输出流进行一系列的flush，保证数据落地磁盘
            // 之前用DataOutputStream输出的数据都是进入了BufferedOutputStream的缓冲区
            // 所以在这里进行一次flush，数据就是进入底层的FileOutputStream
            bufferedOutputStream.flush();
            // 再对FileOutputStream进行一次flush，保证数据进入os cache里
            fileOutputStream.flush();
            // 再次基于FileChannel进行强制性flush，保证数据落地到磁盘上去
            fileOutputStream.getChannel().force(false);
        } catch(Exception e) {
            LOGGER.error("persist slots allocation error......", e);
            return false;
        }
        return true;
    }

}
