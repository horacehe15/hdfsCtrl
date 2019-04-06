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
 * HDFS�Ŀͻ���api�����޷Ǿ���ȥ����hdfs�ϵ��ļ����ļ���
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
		conf.set("dfs.replication", "2");//�������2��ǰ��3��ֵ�������� ��˼���ļ���������
		conf.set("dfs.blocksize", "32m");//�ֿ�32M
		conf.set("fs.permissions.umask-mode", "100");

		// ��һ���� Ҫ����һ��hdfs�Ŀͻ��˶���
		hdfsClient = FileSystem.get(new URI("hdfs://192.168.33.100:9000/"), conf, "root");

	}

	/**
	 * ����ȡ�ļ�
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testGetFile() throws IllegalArgumentException, IOException {

		// hdfs�ͻ��˴�hdfs�϶�ȡ����д�뱾�ش���ʱ������ѡ��ʹ��hadoop�Լ������ı��ؿ���������Ҳ����ʹ��java��ԭ���������������ļ�
		// �������� �� useRawLocalFileSystem�����Ϊtrue����ʹ��javaԭ���⣬����ʹ��hadoop�Լ��ı��ؿ�

		// ���Ҫ����hadoop�Լ��ı��ؿ����������ش����ļ�����Ҫ�ڱ���ϵͳ������һ��hadoop�ı��ؿ�Ŀ¼������������
									//false��Ҫɾ��ԭ								ʹ�ñ��ؿ�
		hdfsClient.copyToLocalFile(false, new Path("/Foxmai.exe"), new Path("e:/"), true);
		hdfsClient.close();

	}

	/**
	 * ����Ŀ¼
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testMkdir() throws IllegalArgumentException, IOException {

		// ��������ļ���ʱ��ָ��Ȩ����Ϣ���������Ľ���������ָ������Ϣ��ȫһ�£���Ϊָ������Ϣ���ᾭ��һ������ֵ���������㣺
		// fs.permissions.umask-mode,Ĭ��ֵ��022����ˣ���Ĭ������»Ḳ�ǵ���������˵�wȨ��
		// hdfsClient.mkdirs(new Path("/xxx2/yyy2"), new FsPermission((short)
		// 700));
		hdfsClient.mkdirs(new Path("/xxx5/yyy2"), new FsPermission("666"));
		hdfsClient.close();

	}

	/**
	 * ɾ���ļ���
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testDeldir() throws IllegalArgumentException, IOException {

		hdfsClient.delete(new Path("/xxx2"), true);
		hdfsClient.close();
	}

	/**
	 * ���ļ���������
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRenamedir() throws IllegalArgumentException, IOException {

		hdfsClient.rename(new Path("/xxx"), new Path("/xxxu"));
		hdfsClient.close();
	}

	/**
	 * �ݹ��ѯָ��·���µ������ļ���Ϣ hdfsClient.listFiles
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
			System.out.println("�������ʱ�䣺" + file.getAccessTime());
			System.out.println("�ļ��ķֿ��С��" + file.getBlockSize());
			System.out.println("�ļ��������飺" + file.getGroup());
			System.out.println("�ļ����ܳ��ȣ�" + file.getLen());
			System.out.println("�ļ�������޸�ʱ�䣺" + file.getModificationTime());
			System.out.println("�ļ��������ߣ�" + file.getOwner());
			System.out.println("�ļ��ĸ�������" + file.getReplication());
			System.out.println("�ļ��Ŀ��λ����Ϣ��" + Arrays.toString(file.getBlockLocations()));
			System.out.println("�ļ���ȫ·����" + file.getPath());
			System.out.println("�ļ���Ȩ����Ϣ��" + file.getPermission());
			System.out.println("----------------------------------------");
		}

		hdfsClient.close();
	}

	/**
	 * ��ѯָ��·���µ������ļ��л����ļ��ڵ���ϢhdfsClient.listStatus
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
			System.out.println((file.isDirectory() ? "d" : "f"));//������ļ���ӡf ������ļ��д�ӡd
			System.out.println("----------------------------------------");
		}
		hdfsClient.close();
	}

	/**
	 * ��ȡ�ļ���ָ��ƫ������Χ������,���ļ���0ƫ��������20���ֽ�
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
	 * ���ļ���20ƫ������ʼ��20���ֽ�
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testReadFilePart2() throws IllegalArgumentException, IOException {

		FSDataInputStream hdfsIn = hdfsClient.open(new Path("/qingshu.txt"));
		// ����ȡ��λ������Ϊָ������ʼλ��
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
	 * ��������ķ�ʽ��hdfs��д����
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

		// ��һ���� Ҫ����һ��hdfs�Ŀͻ��˶���
		FileSystem hdfsClient = FileSystem.get(new URI("hdfs://192.168.33.100:9000/"), conf, "root");

		// ���ԣ��ϴ�һ���ļ�
		hdfsClient.copyFromLocalFile(new Path("d:/FoxmailSetup_7.2.9.116.exe"), new Path("/Foxmai.exe"));
		hdfsClient.close();

	}

}
