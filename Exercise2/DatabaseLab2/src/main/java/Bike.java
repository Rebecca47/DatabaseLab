import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class Bike {

    public static void main(String[] args) {
        Bike bike = new Bike();

        long start = 0;
        long end = 0;
        System.out.println("Start createTables...");
        start = System.currentTimeMillis();
        bike.createTables();
        end = System.currentTimeMillis();
        System.out.println("End createTables! time : " + (end - start) + " ms");

        System.out.println("Start insertData...");
        start = System.currentTimeMillis();
        try {
            bike.insertData();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        end = System.currentTimeMillis();
        System.out.println("End insertData! time : " + (end - start) + " ms");

        System.out.println("Start updatePrice...");
        start = System.currentTimeMillis();
        bike.updatePrice();
        end = System.currentTimeMillis();
        System.out.println("End updatePrice! time : " + (end - start) + " ms");

        System.out.println("Start updateHomeAddress...");
        start = System.currentTimeMillis();
        bike.updateHomeAddress();
        end = System.currentTimeMillis();
        System.out.println("End updateHomeAddress! time : " + (end - start) + " ms");

        System.out.println("Start createEvent...");
        start = System.currentTimeMillis();
        bike.createEvent();
        end = System.currentTimeMillis();
        System.out.println("End createEvent! time : " + (end - start) + " ms");
    }

    private Connection conn;

    /**
     * 构造方法
     */
    public Bike() {
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
        String bike_dropifexists = "DROP TABLE IF EXISTS bike;";
        String bikeCreate = "CREATE TABLE bike (\n" +
                "  b_id INT NOT NULL PRIMARY KEY\n" +
                ")\n" +
                "  DEFAULT CHARSET = utf8;";
        String place_dropifexists = "DROP TABLE IF EXISTS place;";
        String placeCreate = "CREATE TABLE place (\n" +
                "  p_id   INT          NOT NULL PRIMARY KEY AUTO_INCREMENT,\n" +
                "  p_name VARCHAR(255) NOT NULL\n" +
                ")\n" +
                "  DEFAULT CHARSET = utf8;";
        String user_dropifexists = "DROP TABLE IF EXISTS user;";
        String userCreate = "CREATE TABLE user (\n" +
                "  u_id     INT          NOT NULL PRIMARY KEY,\n" +
                "  u_phone  VARCHAR(255) NOT NULL,\n" +
                "  u_name   VARCHAR(255) NOT NULL,\n" +
                "  u_amount FLOAT,\n" +
                "  u_p_id   INT,\n" +
                "  FOREIGN KEY (u_p_id) REFERENCES place (p_id)\n" +
                ")\n" +
                "  DEFAULT CHARSET = utf8;";
        String record_dropifexists = "DROP TABLE IF EXISTS record;";
        String recordCreate = "CREATE TABLE record (\n" +
                "  r_id         INT      NOT NULL PRIMARY KEY AUTO_INCREMENT,\n" +
                "  r_start_p_id INT      NOT NULL,\n" +
                "  r_end_p_id   INT      NOT NULL,\n" +
                "  r_start_time DATETIME NOT NULL,\n" +
                "  r_end_time   DATETIME NOT NULL,\n" +
                "  r_price      FLOAT,\n" +
                "  r_u_id       INT      NOT NULL,\n" +
                "  r_b_id       INT      NOT NULL,\n" +
                "  FOREIGN KEY (r_u_id) REFERENCES user (u_id),\n" +
                "  FOREIGN KEY (r_b_id) REFERENCES bike (b_id),\n" +
                "  FOREIGN KEY (r_start_p_id) REFERENCES place (p_id),\n" +
                "  FOREIGN KEY (r_end_p_id) REFERENCES place (p_id)\n" +
                ")\n" +
                "  DEFAULT CHARSET = utf8;";
        String repair_dropifexists = "DROP TABLE IF EXISTS repair;";
        String repairCreate = "CREATE TABLE repair (\n" +
                "  repair_b_id INT NOT NULL,\n" +
                "  repair_p_id INT NOT NULL,\n" +
                "  FOREIGN KEY (repair_b_id) REFERENCES bike (b_id),\n" +
                "  FOREIGN KEY (repair_p_id) REFERENCES place (p_id),\n" +
                "  PRIMARY KEY (repair_b_id, repair_p_id)\n" +
                ")\n" +
                "  DEFAULT CHARSET = utf8;";

        executeSQLs(new String[]{record_dropifexists, repair_dropifexists, user_dropifexists, bike_dropifexists, place_dropifexists});
        executeSQLs(new String[]{bikeCreate, placeCreate, userCreate, recordCreate, repairCreate});
    }

    /**
     * 插入所有数据
     */
    private void insertData() throws ParseException {
        // Read Bike
        StringBuilder insertBikeSb = new StringBuilder();
        insertBikeSb.append("INSERT INTO bike (b_id) VALUES");
        ArrayList<String> stringArrays = readFile("src/main/resources/bike/bike.txt");
        for (String resultStrings : stringArrays) {
            insertBikeSb.append(" (" + resultStrings + "),");
        }
        insertBikeSb.setCharAt(insertBikeSb.length() - 1, ';');
        executeSQLs(new String[]{insertBikeSb.toString()});

        // Read Record
        Set<String> placeSet = new HashSet<>();

        ArrayList<String[]> recordStringArray = new ArrayList<>();
        stringArrays = readFile("src/main/resources/bike/record.txt");
        for (String resultString : stringArrays) {
            String[] recordInfo = resultString.split(";");
            recordStringArray.add(recordInfo);

            placeSet.add(recordInfo[2]);
            placeSet.add(recordInfo[4]);
        }

        // Insert Place
        HashMap<String, Integer> placeIDMap = new HashMap<>();
        Integer placeCount = 1;

        StringBuilder insertPlaceSb = new StringBuilder();
        insertPlaceSb.append("INSERT INTO place (p_name) VALUES");
        for (String placeName : placeSet) {
            insertPlaceSb.append(" ('" + placeName + "'),");
            placeIDMap.put(placeName, placeCount++);
        }
        insertPlaceSb.setCharAt(insertPlaceSb.length() - 1, ';');
        executeSQLs(new String[]{insertPlaceSb.toString()});

        // Read User

        stringArrays = readFile("src/main/resources/bike/user.txt");

        String[] insertUserSQLs = new String[1 + (stringArrays.size() / 10000)];

        Integer userCounter = 0;
        Integer batchNumber = 0;
        StringBuilder insertUserSb = new StringBuilder();
        insertUserSb.append("INSERT INTO user (u_id, u_phone, u_name, u_amount) VALUES");

        for (String resultString : stringArrays) {
            String[] userInfo = resultString.split(";");
            insertUserSb.append(" (" + userInfo[0] + ", '" + userInfo[2] + "', '" + userInfo[1] + "', " + userInfo[3] + "),");
            userCounter++;
            if (userCounter == 10000) {
                insertUserSb.setCharAt(insertUserSb.length() - 1, ';');
                insertUserSQLs[batchNumber++] = insertUserSb.toString();
                insertUserSb = new StringBuilder();
                insertUserSb.append("INSERT INTO user (u_id, u_phone, u_name, u_amount) VALUES");
                userCounter = 0;
            }
        }
        executeSQLs(insertUserSQLs);

        // Insert Records
        String[] insertRecordSQLs = new String[1 + (recordStringArray.size() / 10000)];

        Integer recordCounter = 0;
        batchNumber = 0;
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
        DateFormat sqlDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        StringBuilder insertRecordSb = new StringBuilder();
        insertRecordSb.append("INSERT INTO record (r_start_p_id, r_end_p_id, r_start_time, r_end_time, r_u_id, r_b_id) VALUES");

        for (String[] recordInfo : recordStringArray) {
            String startTimeString = sqlDateFormat.format(dateFormat.parse(recordInfo[3]));
            String endTimeString = sqlDateFormat.format(dateFormat.parse(recordInfo[5]));


            insertRecordSb.append(" (" + placeIDMap.get(recordInfo[2]) + ", " + placeIDMap.get(recordInfo[4]) + ", '" + startTimeString + "', '" + endTimeString + "', " + Integer.parseInt(recordInfo[0].replace("\uFEFF", "")) + ", " + recordInfo[1] + "),");
            recordCounter++;
            if (recordCounter == 10000) {
                insertRecordSb.setCharAt(insertRecordSb.length() - 1, ';');
                insertRecordSQLs[batchNumber++] = insertRecordSb.toString();
                insertRecordSb = new StringBuilder();
                insertRecordSb.append("INSERT INTO record (r_start_p_id, r_end_p_id, r_start_time, r_end_time, r_u_id, r_b_id) VALUES");
                recordCounter = 0;
            }
        }
        executeSQLs(insertRecordSQLs);
    }

    /**
     * 根据单车使用记录估计用户家庭住址
     */
    private void updateHomeAddress() {
        String updateHomeAddressSQL = "UPDATE `user` u,\n" +
                "  (\n" +
                "    SELECT\n" +
                "      t1.pid pid,\n" +
                "      t1.uid uid\n" +
                "    FROM (\n" +
                "           SELECT\n" +
                "             count(*)     c,\n" +
                "             r.r_u_id     uid,\n" +
                "             r.r_end_p_id pid\n" +
                "           FROM `record` r\n" +
                "           WHERE HOUR(r_end_time) >= 18 AND HOUR(r_end_time) <= 24\n" +
                "           GROUP BY r_u_id, r_end_p_id) t1\n" +
                "    GROUP BY t1.uid, t1.pid\n" +
                "    ORDER BY t1.c DESC) table2\n" +
                "SET u.u_p_id = table2.pid\n" +
                "WHERE u.u_id = table2.uid;";
        executeSQL(updateHomeAddressSQL);
    }

    /**
     * 根据单车使用记录中的使用时间，自动补全费用字段，并在用户账户中，扣除相应的金额
     */
    private void updatePrice() {
        String updateRecordPriceSQL = "UPDATE `record`\n" +
                "SET r_price = if(minute(r_end_time) - minute(r_start_time) < 30, 1, if(\n" +
                "    minute(r_end_time) - minute(r_start_time) < 60, 2, if(minute(r_end_time) - minute(r_start_time) < 90, 3, 4)));";
        String updateUserPriceSQL = "UPDATE `user` u\n" +
                "SET u.u_amount = u.u_amount - (\n" +
                "  SELECT SUM(r_price)\n" +
                "  FROM `record` r\n" +
                "  WHERE r.r_u_id = u.u_id\n" +
                ");";
        executeSQLs(new String[]{updateRecordPriceSQL, updateUserPriceSQL});
    }

    /**
     * 月初，禁用上一个月内使用超200小时的单车
     */
    private void createEvent() {
        String dropEventSQL = "DROP EVENT IF EXISTS `updateUnavailableBike`;";

        String createEventSQL = "CREATE DEFINER =`root`@`localhost` EVENT `updateUnavailableBike`\n" +
                "  ON SCHEDULE EVERY 1 MONTH STARTS '2017-11-01 00:00:00'\n" +
                "  ON COMPLETION NOT PRESERVE ENABLE DO\n" +
                "\n" +
                "  BEGIN\n" +
                "    INSERT INTO `repair`\n" +
                "      SELECT\n" +
                "        repairResult.rbid,\n" +
                "        repairResult.repid\n" +
                "      FROM\n" +
                "        (SELECT\n" +
                "           r1.r_b_id     rbid,\n" +
                "           r1.r_end_p_id repid,\n" +
                "           r1.r_end_time retime\n" +
                "         FROM `record` r1,\n" +
                "           (SELECT\n" +
                "              r.r_b_id                                                                              bid,\n" +
                "              max(r.r_end_time)                                                                     etime,\n" +
                "              sum(minute(r.r_end_time - r.r_start_time) + 60 * hour(r.r_end_time - r.r_start_time)) totalminute\n" +
                "            FROM `record` r\n" +
                "            GROUP BY bid\n" +
                "            HAVING totalminute > 200 * 60) unavailableBikes\n" +
                "         WHERE r1.r_b_id = unavailableBikes.bid AND r1.r_end_time = unavailableBikes.etime\n" +
                "         GROUP BY rbid, repid) repairResult;\n" +
                "  END\n";
        executeSQLs(new String[]{dropEventSQL, createEventSQL});
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
    private ArrayList<String> readFile(String filePath) {
        ArrayList<String> list = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(filePath), "utf-8"));) {
            String temp;
            while ((temp = br.readLine()) != null) {
                list.add(temp);
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
