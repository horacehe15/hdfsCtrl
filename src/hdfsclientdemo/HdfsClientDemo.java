package hdfsclientdemo;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.Test;

/**
 * HDFS的客户端api功能无非就是去操作hdfs上的文件或文件夹
 * 
 * @author ThinkPad
 *
 */
public class HdfsClientDemo {
	FileSystem hdfsClient;

	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("myconf.xml");
		conf.set("dfs.replication", "2");//这个参数2把前面3的值给覆盖了 意思是文件副本个数
		conf.set("dfs.blocksize", "32m");//分块32M
		conf.set("fs.permissions.umask-mode", "100");

		// 第一步： 要创建一个hdfs的客户端对象
		hdfsClient = FileSystem.get(new URI("hdfs://192.168.33.100:9000/"), conf, "root");

	}

	/**
	 * 测试取文件
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testGetFile() throws IllegalArgumentException, IOException {

		// hdfs客户端从hdfs上读取数据写入本地磁盘时，可以选择使用hadoop自己开发的本地库来操作，也可以使用java的原生库来操作本地文件
		// 参数就是 ： useRawLocalFileSystem，如果为true，则使用java原生库，否则，使用hadoop自己的本地库

		// 如果要是用hadoop自己的本地库来操作本地磁盘文件，需要在本地系统中配置一下hadoop的本地库目录到环境变量中
									//false不要删除原								使用本地库
		hdfsClient.copyToLocalFile(false, new Path("/Foxmai.exe"), new Path("e:/"), true);
		hdfsClient.close();

	}

	/**
	 * 创建目录
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testMkdir() throws IllegalArgumentException, IOException {

		// 如果创建文件夹时，指定权限信息，但创建的结果并不会跟指定的信息完全一致，因为指定的信息还会经过一个参数值的掩码运算：
		// fs.permissions.umask-mode,默认值是022，因此，在默认情况下会覆盖掉组和其他人的w权限
		// hdfsClient.mkdirs(new Path("/xxx2/yyy2"), new FsPermission((short)
		// 700));
		hdfsClient.mkdirs(new Path("/xxx5/yyy2"), new FsPermission("666"));
		hdfsClient.close();

	}

	/**
	 * 删除文件夹
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testDeldir() throws IllegalArgumentException, IOException {

		hdfsClient.delete(new Path("/xxx2"), true);
		hdfsClient.close();
	}

	/**
	 * 对文件夹重命名
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRenamedir() throws IllegalArgumentException, IOException {

		hdfsClient.rename(new Path("/xxx"), new Path("/xxxu"));
		hdfsClient.close();
	}

	/**
	 * 递归查询指定路径下的所有文件信息 hdfsClient.listFiles
	 * 
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void listDir() throws FileNotFoundException, IllegalArgumentException, IOException {

		RemoteIterator<LocatedFileStatus> files = hdfsClient.listFiles(new Path("/"), true);
		while (files.hasNext()) {

			LocatedFileStatus file = files.next();
			System.out.println("最近访问时间：" + file.getAccessTime());
			System.out.println("文件的分块大小：" + file.getBlockSize());
			System.out.println("文件的所属组：" + file.getGroup());
			System.out.println("文件的总长度：" + file.getLen());
			System.out.println("文件的最近修改时间：" + file.getModificationTime());
			System.out.println("文件的所属者：" + file.getOwner());
			System.out.println("文件的副本数：" + file.getReplication());
			System.out.println("文件的块的位置信息：" + Arrays.toString(file.getBlockLocations()));
			System.out.println("文件的全路径：" + file.getPath());
			System.out.println("文件的权限信息：" + file.getPermission());
			System.out.println("----------------------------------------");
		}

		hdfsClient.close();
	}

	/**
	 * 查询指定路径下的所有文件夹或者文件节点信息hdfsClient.listStatus
	 * 
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testListDir2() throws FileNotFoundException, IllegalArgumentException, IOException {

		FileStatus[] listStatus = hdfsClient.listStatus(new Path("/"));
		for (FileStatus file : listStatus) {
			System.out.println(file.getPath());
			System.out.println((file.isDirectory() ? "d" : "f"));//如果是文件打印f 如果是文件夹打印d
			System.out.println("----------------------------------------");
		}
		hdfsClient.close();
	}

	/**
	 * 读取文件中指定偏移量范围的数据,从文件的0偏移量，读20个字节
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testReadFilePart1() throws IllegalArgumentException, IOException {

		FSDataInputStream hdfsIn = hdfsClient.open(new Path("/qingshu.txt"));
		FileOutputStream localOut = new FileOutputStream("d:/qingshu-part.txt");
		byte[] b = new byte[10];
		int len = 0;
		long count = 0;
		while ((len = hdfsIn.read(b)) != -1) {
			localOut.write(b);
			count += len;
			if (count == 20)
				break;

		}
		localOut.flush();
		localOut.close();
		hdfsIn.close();
		hdfsClient.close();

	}

	/**
	 * 从文件的20偏移量开始读20个字节
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testReadFilePart2() throws IllegalArgumentException, IOException {

		FSDataInputStream hdfsIn = hdfsClient.open(new Path("/qingshu.txt"));
		// 将读取的位置设置为指定的起始位置
		hdfsIn.seek(20);

		FileOutputStream localOut = new FileOutputStream("d:/qingshu-part2.txt");
		byte[] b = new byte[10];
		int len = 0;
		long count = 0;
		while ((len = hdfsIn.read(b)) != -1) {
			localOut.write(b);
			count += len;
			if (count == 20)
				break;

		}
		localOut.flush();
		localOut.close();
		hdfsIn.close();
		hdfsClient.close();

	}

	/**
	 * 用输出流的方式往hdfs中写数据
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testWriteDataToHdfsFile() throws IllegalArgumentException, IOException {

		FSDataOutputStream hdfsOut = hdfsClient.create(new Path("/access.log"), false);
		hdfsOut.write("6666666666666666666666666666666666".getBytes());
		hdfsOut.write("8888888888888888888888888888888888".getBytes());

		hdfsOut.flush();
		hdfsOut.close();
		hdfsClient.close();

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("myconf.xml");
		conf.set("dfs.replication", "2");
		conf.set("dfs.blocksize", "32m");

		// 第一步： 要创建一个hdfs的客户端对象
		FileSystem hdfsClient = FileSystem.get(new URI("hdfs://192.168.33.100:9000/"), conf, "root");

		// 测试：上传一个文件
		hdfsClient.copyFromLocalFile(new Path("d:/FoxmailSetup_7.2.9.116.exe"), new Path("/Foxmai.exe"));
		hdfsClient.close();

	}

}
