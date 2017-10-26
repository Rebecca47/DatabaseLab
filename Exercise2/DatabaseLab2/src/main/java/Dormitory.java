import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Pattern;

public class Dormitory {

    public static void main(String[] args) {
        Dormitory dormitory = new Dormitory();

        long start = 0;
        long end = 0;
        System.out.println("Start createTables...");
        start = System.currentTimeMillis();
        dormitory.createTables();
        end = System.currentTimeMillis();
        System.out.println("End createTables! time : " + (end - start) + " ms");

        System.out.println("Start insertData...");
        start = System.currentTimeMillis();
        dormitory.insertData();
        end = System.currentTimeMillis();
        System.out.println("End insertData! time : " + (end - start) + " ms");

        System.out.println("Start findWangxiaoxing...");
        start = System.currentTimeMillis();
        dormitory.findWangxiaoxing();
        end = System.currentTimeMillis();
        System.out.println("End findWangxiaoxing! time : " + (end - start) + " ms");

        System.out.println("Start modifyPrice...");
        start = System.currentTimeMillis();
        dormitory.modifyPrice();
        end = System.currentTimeMillis();
        System.out.println("End modifyPrice! time : " + (end - start) + " ms");

        System.out.println("Start exchangeDormitory...");
        start = System.currentTimeMillis();
        dormitory.exchangeDormitory();
        end = System.currentTimeMillis();
        System.out.println("End exchangeDormitory! time : " + (end - start) + " ms");
    }

    private Connection conn;

    /**
     * 构造方法
     */
    public Dormitory() {
        try {
            conn = this.getMysqlConnection();
            conn.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 建表
     */
    private void createTables() {
        String building_dropifexists = "DROP TABLE IF EXISTS building;";
        String buildingCreate = "CREATE TABLE building (" +
                "  b_id     INT PRIMARY KEY AUTO_INCREMENT," +
                "  b_name   VARCHAR(255) NOT NULL," +
                "  b_campus VARCHAR(255) NOT NULL," +
                "  b_price  INT          NOT NULL," +
                "  b_phone  VARCHAR(8)   NOT NULL" +
                ")" +
                "  DEFAULT CHARSET = utf8;";
        String department_dropifexists = "DROP TABLE IF EXISTS department;";
        String departmentCreate = "CREATE TABLE department (" +
                "  d_id   INT PRIMARY KEY AUTO_INCREMENT," +
                "  d_name VARCHAR(255) NOT NULL" +
                ")" +
                "  DEFAULT CHARSET = utf8;";
        String student_dropifexists = "DROP TABLE IF EXISTS student;";
        String studentCreate = "CREATE TABLE student (\n" +
                "  s_id     VARCHAR(255) NOT NULL PRIMARY KEY," +
                "  s_name   VARCHAR(255) NOT NULL," +
                "  s_gender INT          NOT NULL," +
                "  s_d_id   INT          NOT NULL," +
                "  s_b_id   INT          NOT NULL," +
                "  FOREIGN KEY (s_d_id) REFERENCES department (d_id)," +
                "  FOREIGN KEY (s_b_id) REFERENCES building (b_id)" +
                ")" +
                "  DEFAULT CHARSET = utf8;";

        executeSQLs(new String[]{student_dropifexists, building_dropifexists, department_dropifexists});
        executeSQLs(new String[]{buildingCreate, departmentCreate, studentCreate});
    }

    /**
     * 插入所有数据
     */
    private void insertData() {
        // Read Dormitory
        HashMap<String, String> dormitoryPhoneMap = new HashMap<>();
        ArrayList<String[]> stringArrays = readFile("Src/main/resources/dormitory/phone.txt");
        for (String[] resultStrings : stringArrays) {
            for (String s : resultStrings) {
                String[] phoneArray = s.split(";");
                if (phoneArray.length == 2) {
                    dormitoryPhoneMap.put(phoneArray[0], phoneArray[1]);
                }
            }
        }

        // Read Student
        ArrayList<String[]> studentStringArray = new ArrayList<>();
        stringArrays = readFile("Src/main/resources/dormitory/dormitory.csv");
        for (String[] resultStrings : stringArrays) {
            for (String s : resultStrings) {
                if (isNumeric(s.split(",")[6])) {
                    studentStringArray.add(s.split(","));
                }
            }
        }

        // Insert Department
        HashSet<String> departmentNameSet = new HashSet<>();
        for (String[] strings : studentStringArray) {
            departmentNameSet.add(strings[0]);
        }

        HashMap<String, Integer> departmentIdMap = new HashMap<>();
        Integer departmentCount = 1;

        for (String department : departmentNameSet) {
            executeSQL("INSERT INTO department (d_name) VALUES ('" + department + "');");
            departmentIdMap.put(department, departmentCount++);
        }

        // Insert Buildings
        HashMap<String, String[]> buildingMaps = new HashMap<>();
        for (String[] strings : studentStringArray) {
            buildingMaps.put(strings[5], new String[]{strings[4], strings[6]}); // 4: campus，6: price
        }

        HashMap<String, Integer> buildingIdMap = new HashMap<>();
        Integer buildingCount = 1;

        StringBuilder insertBuildingSb = new StringBuilder();
        insertBuildingSb.append("INSERT INTO building (b_name, b_campus, b_price, b_phone) VALUES");
        for (String key : buildingMaps.keySet()) {
            String campus = buildingMaps.get(key)[0];
            String price = buildingMaps.get(key)[1];
            insertBuildingSb.append(" ('" + key + "', '" + campus + "', " + price + ", '" + dormitoryPhoneMap.get(key) + "'),");

            buildingIdMap.put(key, buildingCount++);
        }
        insertBuildingSb.setCharAt(insertBuildingSb.length() - 1, ';');
        executeSQL(insertBuildingSb.toString());


        // Insert Students
        StringBuilder insertStudentSb = new StringBuilder();
        insertStudentSb.append("INSERT into student (s_id, s_name, s_gender, s_d_id, s_b_id) VALUES");
        for (String[] strings : studentStringArray) {
            insertStudentSb.append(" ('" + strings[1] + "', '" + strings[2] + "', " + (strings[3].equals("男") ? 0 : 1) + ", '" + departmentIdMap.get(strings[0]) + "', '" + buildingIdMap.get(strings[5]) + "'),");
        }
        insertStudentSb.setCharAt(insertStudentSb.length() - 1, ';');
        executeSQL(insertStudentSb.toString());
    }

    /**
     * 查询“王小星”同学所在宿舍楼的所有院系
     */
    private void findWangxiaoxing() {
        String findWangxiaoxingSQL = "SELECT *\n" +
                "FROM `department`\n" +
                "WHERE d_id IN (\n" +
                "  SELECT s_d_id\n" +
                "  FROM student s\n" +
                "  WHERE s.s_b_id = (SELECT s_b_id\n" +
                "                    FROM `student`\n" +
                "                    WHERE s_name = '王小星'));";
        executeSQL(findWangxiaoxingSQL);
    }

    /**
     * 调整陶园一舍的住宿费用提高至 1200 元
     */
    private void modifyPrice() {
        String modifyPriceSQL = "\n" +
                "UPDATE `building`\n" +
                "SET b_price = 1200\n" +
                "WHERE b_name = \"陶园1舍\";";
        executeSQL(modifyPriceSQL);
    }

    /**
     * 软件学院男女研究生互换宿舍楼
     */
    private void exchangeDormitory() {
        String saveDormitoryIDSQL = "SELECT d_id\n" +
                "INTO @software_id\n" +
                "FROM `department`\n" +
                "WHERE d_name = '软件学院'\n" +
                "LIMIT 1;";

        String saveMaleIDSQL = "SELECT s.s_b_id\n" +
                "INTO @male_bid\n" +
                "FROM `student` s\n" +
                "WHERE s.s_d_id = @software_id AND s.s_gender = 0\n" +
                "LIMIT 1;";

        String saveFemaleIDSQL = "SELECT s.s_b_id\n" +
                "INTO @female_bid\n" +
                "FROM `student` s\n" +
                "WHERE s.s_d_id = @software_id AND s.s_gender = 1\n" +
                "LIMIT 1;";

        String updateMaleSQL = "UPDATE `student`\n" +
                "SET s_b_id = @female_bid\n" +
                "WHERE s_d_id = @software_id AND s_gender = 0;";

        String updateFemaleSQL = "UPDATE `student`\n" +
                "SET s_b_id = @male_bid\n" +
                "WHERE s_d_id = @software_id AND s_gender = 1;";

        executeSQL(saveDormitoryIDSQL);
        executeSQL(saveMaleIDSQL);
        executeSQL(saveFemaleIDSQL);
        executeSQLs(new String[]{updateMaleSQL, updateFemaleSQL});

        //或者以下方法，但是时间长

//        String exchangeDormitorySQL = "UPDATE student AS s1\n" +
//                "  JOIN student AS s2\n" +
//                "  JOIN department AS d\n" +
//                "    ON s1.s_gender = 0 AND s2.s_gender = 1 AND s1.s_d_id = s2.s_d_id AND s1.s_d_id = d.d_id AND d.d_name = '软件学院'\n" +
//                "SET s1.s_b_id = s2.s_b_id, s2.s_b_id = s1.s_b_id;";
//        executeSQL(exchangeDormitorySQL);
    }

    /**
     * 执行一组 SQL 语句
     *
     * @param statements SQL 语句数组
     */
    private void executeSQLs(String[] statements) {
        try (Statement statement = conn.createStatement()) {
            for (String s : statements) {
                statement.addBatch(s);
            }
            statement.executeBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    /**
     * 执行一句 SQL 语句
     *
     * @param sql
     */
    private void executeSQL(String sql) {
        try (Statement statement = conn.createStatement()) {
            statement.execute(sql);
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    /**
     * 读取文件
     *
     * @param filePath 文件路径
     * @return
     */
    private ArrayList<String[]> readFile(String filePath) {
        ArrayList<String[]> list = new ArrayList<String[]>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(filePath), "utf-8"));) {
            String temp = null;
            while ((temp = br.readLine()) != null) {
                list.add(temp.split("\t"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }

    /**
     * get mysql connection
     *
     * @return
     * @throws Exception
     */
    public Connection getMysqlConnection() throws Exception {
        String mysqlIP = "127.0.0.1";
        String mysqlPort = "3306";
        String dbName = "Homework2";
        String loginName = "root";
        String password = "songkuixi";
        String url = "jdbc:mysql://" + mysqlIP + ":" + mysqlPort + "/" + dbName
                + "?" + "user=" + loginName + "&password=" + password
                + "&useUnicode=true&characterEncoding=UTF8";

        // load class driver
        Class.forName("com.mysql.jdbc.Driver");

        // return connection
        return DriverManager.getConnection(url);
    }

    public boolean isNumeric(String str) {
        return Pattern.compile("[0-9]*").matcher(str).matches();
    }

}
