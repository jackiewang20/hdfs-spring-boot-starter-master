package com.example.hdfs.starter.component;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * hdfs系统文件操作
 *
 * @author jackie wang
 * @since 2021/3/9 19:21
 */
public class HdfsService {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private Configuration configuration;
    private FileSystem fileSystem;

    private static final int bufferSize = 1024 * 1024 * 64;
    private static final long FILE_SIZE_LIMIT = 1024 * 1024 * 30;
    private static List<String> ARRAY_BLACK_LIST = new ArrayList<>(
            Arrays.asList("hbase", "kylin", "tmp", "user", "one"));


    public HdfsService(Configuration configuration) {
        Assert.notNull(configuration, "Configuration can't be empty.");
        this.configuration = configuration;
        try {
            this.fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            throw new RuntimeException("The HDFS file system failed to initialize.", e);
        }
    }

    /**
     * 获取HDFS配置信息
     *
     * @return
     */
    private Configuration getConfiguration() {
        return configuration;
    }

    /**
     * 获取HDFS文件系统对象
     *
     * @return
     * @throws Exception
     */
    public FileSystem getFileSystem() throws Exception {
        return fileSystem;
    }

    private void close(FSDataOutputStream outputStream) {
        try {
            if (outputStream != null) {
                outputStream.close();
            }
        } catch (Exception e) {
            logger.error("outputStream close exception", e);
        }
    }

    private void close(FSDataInputStream inputStream) {
        try {
            if (inputStream != null) {
                inputStream.close();
            }
        } catch (Exception e) {
            logger.error("inputStream close exception", e);
        }
    }

    /**
     * 在HDFS创建文件夹
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean mkdir(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            throw new NullPointerException("Path cannot be empty.");
        }
        if (existFile(path)) {
            return true;
        }
        FileSystem fs = getFileSystem();
        boolean isOk = false;
        try {
            // 目标路径
            Path srcPath = new Path(path);
            isOk = fs.mkdirs(srcPath);
        } catch (IOException e) {
            throw new RuntimeException("[HdfsService#mkdir(String path)]File operation exception", e);
        }
        return isOk;
    }

    /**
     * 判断HDFS文件是否存在
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean existFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            throw new NullPointerException("Path cannot be empty.");
        }
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        boolean isExists = fs.exists(srcPath);
        return isExists;
    }

    /**
     * 读取HDFS目录信息
     *
     * @param path
     * @return
     * @throws Exception
     */
    public List<Map<String, Object>> readPathInfo(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            throw new NullPointerException("Path cannot be empty.");
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();

        // 目标路径
        Path newPath = new Path(path);
        FileStatus[] statusList = fs.listStatus(newPath);
        List<Map<String, Object>> list = new ArrayList<>();
        if (null != statusList && statusList.length > 0) {
            for (FileStatus fileStatus : statusList) {
                Map<String, Object> map = new HashMap<>();
                map.put("filePath", fileStatus.getPath());
                map.put("fileStatus", fileStatus.toString());
                list.add(map);
            }
            return list;
        } else {
            return null;
        }
    }

    /**
     * HDFS上传文件
     *
     * @param path hdfs目录；
     * @param file 本地浏览要上传的文件；
     * @throws Exception
     */
    public void uploadCreateFile(String path, MultipartFile file) throws Exception {
        if (StringUtils.isEmpty(path)) {
            throw new NullPointerException("Path cannot be empty.");
        }

        if (null == file || null == file.getBytes()) {
            throw new NullPointerException("MultipartFile cannot be empty.");
        }

        if (blackList(path)) {
            throw new IOException("No permission to operate. Files in the following directories cannot be updated:" + ARRAY_BLACK_LIST.toString());
        }

        String fileName = file.getOriginalFilename();
        FileSystem fs = getFileSystem();
        FSDataOutputStream outputStream = null;

        try {
            // 上传时默认当前目录，后面自动拼接文件的目录
            Path newPath = new Path(path + "/" + fileName);
            // 打开一个输出流
            outputStream = fs.create(newPath);
            outputStream.write(file.getBytes());
        } catch (IOException e) {
            throw new IOException("[createFile()]File operation exception", e);
        } finally {
            close(outputStream);
        }
    }

    /**
     * 读取HDFS文件内容
     *
     * @param path
     * @return
     * @throws Exception
     */
    public String readFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            throw new NullPointerException("Path cannot be empty.");
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);

        if (fs.getLength(srcPath) > FILE_SIZE_LIMIT) {
            throw new IOException("[readFile(String path)]File size over " + FILE_SIZE_LIMIT);
        }

        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(srcPath);
            // 防止中文乱码
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String lineTxt = "";
            StringBuffer sb = new StringBuffer();
            while ((lineTxt = reader.readLine()) != null) {
                sb.append(lineTxt);
            }
            return sb.toString();
        } finally {
            close(inputStream);
        }
    }

    /**
     * 读取HDFS文件列表
     *
     * @param path
     * @return
     * @throws Exception
     */
    public List<Map<String, String>> listFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            throw new NullPointerException("Path cannot be empty.");
        }
        if (!existFile(path)) {
            return null;
        }

        FileSystem fs = getFileSystem();
        List<Map<String, String>> returnList = new ArrayList<>();

        // 目标路径
        Path srcPath = new Path(path);
        // 递归找到所有文件
        RemoteIterator<LocatedFileStatus> filesList = fs.listFiles(srcPath, true);
        while (filesList.hasNext()) {
            LocatedFileStatus next = filesList.next();
            String fileName = next.getPath().getName();
            Path filePath = next.getPath();
            Map<String, String> map = new HashMap<>();
            map.put("fileName", fileName);
            map.put("filePath", filePath.toString());
            returnList.add(map);
        }

        return returnList;
    }

    /**
     * HDFS重命名文件
     *
     * @param oldFilePath
     * @param newFilePath
     * @return
     * @throws Exception
     */
    public boolean renameFile(String oldFilePath, String newFilePath) throws Exception {
        boolean isOk = false;
        if (StringUtils.isEmpty(oldFilePath)) {
            throw new NullPointerException("OldName cannot be empty.");
        }

        if (StringUtils.isEmpty(newFilePath)) {
            throw new NullPointerException("NewName cannot be empty.");
        }

        FileSystem fs = getFileSystem();

        try {
            // 原文件目标路径
            Path oldPath = new Path(oldFilePath);
            // 重命名目标路径
            Path newPath = new Path(newFilePath);
            isOk = fs.rename(oldPath, newPath);
        } catch (IOException e) {
            throw new IOException("Failed to rename file.", e);
        }
        return isOk;
    }

    /**
     * 黑名单列表
     *
     * @param path
     * @return 返回true，说明在黑名单中
     */
    public Boolean blackList(String path) {
        if (path != null && path.trim().length() > 1) {
            if (!path.substring(0, 1).equals("/")) {
                throw new IllegalArgumentException("Path error: Please use a valid directory prefix, such as: /one");
            }
        }

        Boolean flag = false;

        String[] arrays = path.split("/");
        for (String key : ARRAY_BLACK_LIST) {
            if (key.equals(arrays[1])) {
                return flag = true; // 在黑名单中
            }
        }
        return flag;
    }

    /**
     * 删除HDFS文件
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean deleteFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            throw new NullPointerException("Path cannot be empty.");
        }
        if (!existFile(path)) {
            return false;
        }

        if (blackList(path)) {
            throw new IOException("No permission to operate. Files in the following directories cannot be updated:" + ARRAY_BLACK_LIST.toString());
        }

        FileSystem fs = getFileSystem();
        boolean isOk = false;
        try {
            Path srcPath = new Path(path);
            isOk = fs.delete(srcPath, false);
        } catch (PathIsNotEmptyDirectoryException e) {
            throw new IOException("Directory not empty,deletion failed.Only files and empty directories can be deleted.", e);
        } catch (IOException e) {
            throw new IOException("Failed to deletion file.", e);
        }
        return isOk;
    }

    /**
     * 上传HDFS文件
     *
     * @param localFilePath  本地上传路径；
     * @param uploadFilePath 目标路径
     * @throws Exception
     */
    public void uploadOverwriteFile(String localFilePath, String uploadFilePath) throws Exception {
        if (StringUtils.isEmpty(localFilePath)) {
            throw new NullPointerException("Path cannot be empty.");
        }
        if (StringUtils.isEmpty(uploadFilePath)) {
            throw new NullPointerException("uploadPath cannot be empty.");
        }

        if (blackList(uploadFilePath)) {
            throw new IOException("No permission to operate. Files in the following directories cannot be updated:" + ARRAY_BLACK_LIST.toString());
        }

        FileSystem fs = getFileSystem();
        try {
            // 上传路径
            Path clientPath = new Path(localFilePath);
            // 目标路径
            Path serverPath = new Path(uploadFilePath);

            // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
            fs.copyFromLocalFile(false, clientPath, serverPath);
        } catch (IOException e) {
            throw new IOException("File upload failed.", e);
        }
    }

    /**
     * 下载HDFS文件
     *
     * @param path         hdfs源文件路径；
     * @param downloadPath 下载的本地路径；
     * @throws Exception
     */
    public void downloadFile(String path, String downloadPath) throws Exception {
        if (StringUtils.isEmpty(path)) {
            throw new NullPointerException("Path cannot be empty.");
        }
        if (StringUtils.isEmpty(downloadPath)) {
            throw new NullPointerException("downloadPath cannot be empty.");
        }
        FileSystem fs = getFileSystem();
        try {
            // 上传路径
            Path clientPath = new Path(path);
            // 目标路径
            Path serverPath = new Path(downloadPath);

            // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
            fs.copyToLocalFile(false, clientPath, serverPath);
        } catch (IOException e) {
            throw new IOException("File download failed.", e);
        }
    }

    /**
     * HDFS文件复制
     *
     * @param sourcePath
     * @param targetPath
     * @throws Exception
     */
    public void copyFile(String sourcePath, String targetPath) throws Exception {
        if (StringUtils.isEmpty(sourcePath)) {
            throw new NullPointerException("sourcePath cannot be empty.");
        }
        if (StringUtils.isEmpty(targetPath)) {
            throw new NullPointerException("targetPath cannot be empty.");
        }

        FileSystem fs = getFileSystem();
        // 原始文件路径
        Path oldPath = new Path(sourcePath);
        // 目标路径
        Path newPath = new Path(targetPath);

        FSDataInputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        try {
            inputStream = fs.open(oldPath);
            outputStream = fs.create(newPath);

            IOUtils.copyBytes(inputStream, outputStream, bufferSize, false);
        } catch (IOException e) {
            throw new IOException("File copy failed.", e);
        } finally {
            close(inputStream);
            close(outputStream);
        }
    }

    /**
     * 打开HDFS上的文件并返回byte数组
     *
     * @param path
     * @return
     * @throws Exception
     */
    public byte[] openFileToBytes(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            throw new NullPointerException("path cannot be empty.");
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();

        // 目标路径
        Path srcPath = new Path(path);
        if (fs.isDirectory(srcPath)) {
            throw new RuntimeException("Path error, cannot be directory.");
        }

        try {
            FSDataInputStream inputStream = fs.open(srcPath);
            return IOUtils.readFullyToByteArray(inputStream);
        } catch (IOException e) {
            throw new IOException("File opening failed.", e);
        }
    }

    /**
     * 获取某个文件在HDFS的集群位置
     *
     * @param path
     * @return
     * @throws Exception
     */
    public BlockLocation[] getFileBlockLocations(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            throw new NullPointerException("path cannot be empty.");
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        FileStatus fileStatus = fs.getFileStatus(srcPath);
        if (fileStatus != null && fileStatus.isDirectory()) {
            throw new FileNotFoundException("The current path is a directory.Current request path: " + path);
        }

        BlockLocation[] fileBlockLocations = null;
        try {
            fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        } catch (IOException e) {
            throw new IOException("Failed to get file location.", e);
        }
        return fileBlockLocations;
    }

}