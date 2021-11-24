import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.*;
import java.util.List;
import java.io.IOException;

public class hbase{
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;
    private static ResultScanner scanner;
    static {
        //1.获得Configuration实例并进行相关设置
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //configuration.addResource(HBaseTest.class.getResource("hbase-site.xml"));
        //2.获得Connection实例
        try {
            connection = ConnectionFactory.createConnection(configuration);
            //3.1获得Admin接口
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void close() {
        try{
            if(admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {




        System.out.println("----------task1-----------");
        //task1
        //创建表
        String  familyNames[]={"info","class1","class2","class3"};
        createTable("student",familyNames);
        //向表中插入001数据
        insert("student","2015001","info","S_Name","Li Lei");
        insert("student","2015001","info","S_Sex","male");
        insert("student","2015001","info","S_Age","23");
        insert("student","2015001","class1","C_No","123001");
        insert("student","2015001","class1","C_Name","Math");
        insert("student","2015001","class1","C_Credit","2.0");
        insert("student","2015001","class1","C_Score","86");
        insert("student","2015001","class3","C_No","123003");
        insert("student","2015001","class3","C_Name","English");
        insert("student","2015001","class3","C_Credit","3.0");
        insert("student","2015001","class3","C_Score","69");
        //向表中插入002数据
        insert("student","2015002","info","S_Name","Han Meimei");
        insert("student","2015002","info","S_Sex","female");
        insert("student","2015002","info","S_Age","22");
        insert("student","2015002","class2","C_No","123002");
        insert("student","2015002","class2","C_Name","Computer Science");
        insert("student","2015002","class2","C_Credit","5.0");
        insert("student","2015002","class2","C_Score","77");
        insert("student","2015002","class3","C_No","123003");
        insert("student","2015002","class3","C_Name","English");
        insert("student","2015002","class3","C_Credit","3.0");
        insert("student","2015002","class3","C_Score","99");
        //向表中插入003数据
        insert("student","2015003","info","S_Name","Zhang San");
        insert("student","2015003","info","S_Sex","male");
        insert("student","2015003","info","S_Age","24");
        insert("student","2015003","class1","C_No","123001");
        insert("student","2015003","class1","C_Name","Math");
        insert("student","2015003","class1","C_Credit","2.0");
        insert("student","2015003","class1","C_Score","98");
        insert("student","2015003","class2","C_No","123003");
        insert("student","2015003","class2","C_Name","Computer Science");
        insert("student","2015003","class2","C_Credit","5.0");
        insert("student","2015003","class2","C_Score","95");


        System.out.println(" ");
        System.out.println("----------task2-----------");
        //task2
        //查询选修Class2的学生的成绩scan 'student',{COLUMN=>'class2:C_Score'}
        scanByColumnKey("student","class2","C_Score");


        System.out.println(" ");
        System.out.println("----------task3-----------");
//        scanByColumnKey("student","class2","C_Score");
        //task3
        //增加新的列族和新列，并添加数据
        addColumnFamily("student","Contact");
        insert("student","2015001","Contact","Email","lilei@qq.com");
        insert("student","2015002","Contact","Email","hmm@qq.com");
        insert("student","2015003","Contact","Email","zs@qq.com");


        System.out.println(" ");
        System.out.println("----------task4-----------");
        //task4
        //删除学号为2015003的学生的选课记录
        deleteColumn("student","2015003","class1","C_No");
        deleteColumn("student","2015003","class1","C_Name");
        deleteColumn("student","2015003","class1","C_Credit");
        deleteColumn("student","2015003","class1","C_Score");
        deleteColumn("student","2015003","class2","C_No");
        deleteColumn("student","2015003","class2","C_Name");
        deleteColumn("student","2015003","class2","C_Credit");
        deleteColumn("student","2015003","class2","C_Score");
        deleteColumn("student","2015003","class3","C_No");
        deleteColumn("student","2015003","class3","C_Name");
        deleteColumn("student","2015003","class3","C_Credit");
        deleteColumn("student","2015003","class3","C_Score");

        System.out.println(" ");
        System.out.println("----------task5-----------");
        //tas5
        //删除表
        dropTable("student");

    }
    /**
     * 创建表
     * @param tableName 表名
     * @param familyNames 列族名
     * */
    public static void createTable(String tableName, String familyNames[]) throws IOException {
        //如果表存在退出
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Table exists!");
            return;
        }
        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        }
        //tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        admin.createTable(tableDescriptor);
        System.out.println("createtable success!");
    }

    /**
     * 删除表
     * @param tableName 表名
     * */
    public static void dropTable(String tableName) throws IOException {
        //如果表不存在报异常
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName+"不存在");
            return;
        }

        //删除之前要将表disable
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("deletetable " + tableName + " ok.");
    }

    /**
     * 指定行/列中插入数据
     * @param tableName 表名
     * @param rowKey 主键rowkey
     * @param family 列族
     * @param column 列
     * @param value 值
     * TODO: 批量PUT
     */
    public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        //3.2获得Table接口,需要传入表名
        Table table =connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
    }


    /**
     * 删除表中的指定行
     * @param tableName 表名
     * @param rowKey rowkey
     * TODO: 批量删除
     */
    public static void delete(String tableName, String rowKey) throws IOException {
        //3.2获得Table接口,需要传入表名
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }


    public static void deleteColumn(String tableName,String rowKey,String familyName,String columnName)
    throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println("delete "+rowKey+":"+familyName+":"+columnName+" "+"susscess");

    }

    public static void addColumnFamily(String tableName,String columnName)throws IOException {
        TableName tableName1 = TableName.valueOf(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnName);
        admin.addColumn(tableName1,hColumnDescriptor);
        System.out.println("add column family "+columnName+" ok");
    }


    public static void scanByColumnKey(String tableName,String family,String qualifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 通过列键（family:qualifier）创建扫描器，得到基于列键扫描的数据
        scanner = table.getScanner(Bytes.toBytes(family),Bytes.toBytes(qualifier));
        printScanResults();
    }

    // 格式化打印扫描到的数据
    private static void printScanResults() {
        for (Result row : scanner) {
            System.out.println(row);
            for (Cell cell : row.listCells()) {
                System.out.println(
                        "RowKey:"
                                + Bytes.toString(row.getRow())
                                + " Family:"
                                + Bytes.toString(CellUtil.cloneFamily(cell))
                                + " Qualifier:"
                                + Bytes.toString(CellUtil.cloneQualifier(cell))
                                + " Value:"
                                + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}