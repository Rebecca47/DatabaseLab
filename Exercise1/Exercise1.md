
# 第一次实验

141210026 宋奎熹

## 第一题 

服装店的后台管理系统的部分关系模式如下： 

| 表名 | 模式 |
| :-: | :-: |
| 服装表 | clothes(cid, name, price, brand, type, launchYear) |
| 订单表 | order(oid, cuid, cid, quantity, totalPrice, orderTime) |
| 顾客表 | customer(cuid, cname, phone) |

其中，brand为服装品牌，type为服装种类，launchYear为上市年份，year类型； orderTime为下单时间，dateTime类型 与时间比较相关的mysql函数：date_format, date_sub。

1.请创建表order（要求：oid为主键，其余子段为不能为空）。

```
CREATE TABLE `order` (
  `oid`        INT(11)  NOT NULL AUTO_INCREMENT,
  `cuid`       INT(11)  NOT NULL,
  `cid`        INT(11)  NOT NULL,
  `quantity`   INT(11)  NOT NULL,
  `totalprice` DOUBLE   NOT NULL,
  `ordertime`  DATETIME NOT NULL,
  PRIMARY KEY (`oid`)
);
```

2.查询单价在120元到180元之间（包括120元和150元）的所有衬衫，列出它们的名字和单价，并按照价格递增。 

```
SELECT
  name,
  price
FROM `clothes`
WHERE price >= 120 AND price <= 150 AND type = "衬衫"
ORDER BY price ASC;
```

3.查询【nike】2015年新上市的所有【裤子】至今为止的各自的销量。 

```
SELECT
  clo.cid,
  ifnull(SUM(quantity), 0) salecount
FROM
  (SELECT *
   FROM `clothes`
   WHERE brand = 'nike' AND launchYear = 2015 AND type = '裤子') clo
  LEFT JOIN `order` ON clo.cid = order.cid
GROUP BY clo.cid, price;
```

4.查询顾客【jacky】在2014年11月这个月内购买服装所花的总费用。 

```
SELECT IFNULL(SUM(totalprice), 0)
FROM `order` o
WHERE o.cuid = (
  SELECT cuid
  FROM customer
  WHERE cname = 'jacky'
) AND YEAR(ordertime) = 2014 AND MONTH(ordertime) = 11;
```

5.查询同时购买了【nike】品牌【2015】年新上市的最贵的【外套】和【裤子】的顾客的【姓名】。 

```
SELECT cus.cname
FROM customer cus
WHERE cus.cuid IN (
  SELECT ord1.cuid
  FROM `order` ord1
  WHERE ord1.cid IN (
    SELECT clo_coat.cid
    FROM `clothes` clo_coat
    WHERE clo_coat.price = (
      SELECT max(clo1.price)
      FROM `clothes` clo1
      WHERE clo1.launchYear = 2015 AND clo1.type = '外套' AND clo1.brand = 'nike'
    ) AND clo_coat.type = '外套' AND clo_coat.launchYear = 2015 AND clo_coat.brand = 'nike'
  ) AND cus.cuid IN (
    SELECT ord2.cuid
    FROM `order` ord2
    WHERE ord2.cid IN (
      SELECT clo_pant.cid
      FROM `clothes` clo_pant
      WHERE clo_pant.price = (
        SELECT max(clo2.price)
        FROM `clothes` clo2
        WHERE clo2.launchYear = 2015 AND clo2.type = '裤子' AND clo2.brand = 'nike'
      ) AND clo_pant.launchYear = 2015 AND clo_pant.type = '裤子' AND clo_pant.brand = 'nike'
    )
  )
);
```

6.查询2014.12.12这天销量排名【前三】的服装的【名称】，【销量】以及它们对应的【品牌】。

```
SELECT
  clo.name,
  clo.brand,
  sum(o.quantity)
FROM clothes clo, `order` o
WHERE ordertime BETWEEN '2014-12-12' AND '2014-12-13'
      AND clo.cid = o.cid
GROUP BY clo.brand, clo.name
ORDER BY sum(o.quantity) DESC
LIMIT 3;
``` 

7.查询2014.11.11，在所有购买了nike品牌服装的顾客中，消费金额最大的顾客的姓名和联系电话。 

```
SELECT
  cu.cname,
  cu.phone
FROM `customer` cu
WHERE cu.cuid IN (
  SELECT o.cuid
  FROM `order` o, `clothes` clo
  WHERE o.cid = clo.cid AND clo.brand = 'nike' AND o.ordertime BETWEEN '2014-11-11' AND '2014-11-12'
  GROUP BY o.cuid
  ORDER BY sum(o.totalprice) DESC
)
LIMIT 1;
```

8.查询2014.12.12这天，每个订单消费金额都在800元及以上的顾客的信息。 

```
SELECT *
FROM `customer` cu
WHERE cu.cuid NOT IN (
  SELECT o.cuid
  FROM `order` o
  WHERE o.cuid = cu.cuid AND o.ordertime BETWEEN '2014-11-11' AND '2014-11-12' AND totalprice < 800
) AND cu.cuid IN (
  SELECT o2.cuid
  FROM `order` o2
  WHERE o2.ordertime BETWEEN '2014-11-11' AND '2014-11-12'
);
```

9.删除2015年9月1日之前过去一年内没有消费过的顾客的信息。
    
```
DELETE FROM `customer`
WHERE cuid NOT IN (
  SELECT o.cuid
  FROM `order` o
  WHERE o.ordertime BETWEEN '2014-09-01' AND '2015-09-01'
);
```
 
10.授予销售经理的账号Mike对表customer的更新、插入和查询权限，但不给删除权限。 

```
GRANT UPDATE, INSERT, SELECT ON `Customer` TO 'Mike';
REVOKE DELETE ON `Customer` FROM 'Mike';
```

## 第二题 

已知关系模式：

| 表名 | 模式 |
| :-: | :-: |
| 提交记录表 |Commit(sha,total_add,total_delete,file,datetime,author) |
| 文件更改表 | File(sha, filename, add_line, delete_line, datetime) |
| 迭代表 | Deadline(id, start_day, end_day, name) |
记录提交表中记录了项目代码提交的编号，提交的新增行数，删除行数，日期，提交作者。文件更改表中记录了项目文件提交的编号，文件名，文件增加的行数，文件删除的行数，提交的日期。迭代表中记录了项目每一个迭代的起止时间。

1.删除提交记录表中增加代码行数大于5000行，删除代码行数小于100行的提交记录。

```
DELETE FROM `commit`
WHERE total_add > 5000 AND total_delete < 100;
```

2.查询项目中每一个迭代每一个学生的代码提交数量，显示迭代id，学生姓名，代码行数。 

```
SELECT
  c.author,
  d.id,
  sum(c.total_add) - sum(c.total_delete) codelines
FROM `deadline` d, `commit` c
WHERE c.datetime BETWEEN d.start_day AND d.end_day
GROUP BY c.author, d.id;
```

3.查询项目中所有的java文件占总文件数量的比例，显示java文件的数量，总文件的数量。

```
SELECT count(*) allfilecount, sum(if(allfile.filename LIKE '%.java',1,0)) javafilecount
FROM (
  SELECT f.filename
  FROM `file` f
  GROUP BY f.filename
) allfile;
```

4.查询项目过程中每个迭代中提交代码次数最多的日期，显示迭代号，提交日期，对应日期提交的次数。

```
SELECT
  d.id,
  DATE(c.datetime) commitdate,
  count(*)         commitcount
FROM `commit` c, `deadline` d
WHERE c.datetime BETWEEN d.start_day AND d.end_day
GROUP BY commitdate, d.id
HAVING commitcount >= (
  SELECT count(*) count2
  FROM `commit` c2, `deadline` d2
  WHERE c2.datetime BETWEEN d2.start_day AND d2.end_day AND d2.id = d.id
  GROUP BY Date(c2.datetime)
  ORDER BY count2 DESC
  LIMIT 1
)
ORDER BY commitcount DESC;
```

5.查询所有的文件行数超过200行的java文件（假设每个文件的初始行数为0行），并按照降序排列，显示文件名，文件的代码行数。

```
SELECT
  f.filename,
  sum(f.add_line) - sum(f.delete_line) codelines
FROM `file` f
GROUP BY f.filename
HAVING codelines > 200
ORDER BY codelines DESC;
```

6.更新迭代表中迭代三的结束日期为原来结束日期的一周。

```
UPDATE deadline d
SET d.end_day = DATE_SUB(d.end_day, INTERVAL 1 WEEK)
WHERE d.id = 3;
```

## 第三题

某付费文章阅读平台的部分关系模式如下：

| 表名 | 模式 |
| :-: | :-: |
| 文章作者表 | platform_writer(writer_id, writer_name, writer_email, create_time) |
| 文章表 | platform_article(article_id, writer_id, article_title, content, create_time) |
| 读者表 | platform_reader(reader_id, reader_name, create_time) |
| 付费账单表 | platform_deal(deal_id, article_id, reader_id, deal_payment, create_time) |

其中，create_time为datetime类型，deal_payment为单笔交易的付费金额，double类型，金额多少由读者根据自己的意愿自行输入，每篇文章每个读者只需且只能付费一次。 

1.为数据库来自IP120.55.91.83的用户writer, 密码为writer， 设置文章作者表的增改查权限(该数据库的schema名称为platform)。

```
GRANT INSERT, UPDATE, SELECT ON platform.platform_reader TO 'writer'@'120.55.91.83'
IDENTIFIED BY 'writer';

```

2.查询姓名为zoe的读者最近付费的3篇文章的名称，内容和作者姓名。

```
SELECT
  a.article_title,
  a.content,
  w.writer_name
FROM `platform_article` a, `platform_writer` w, `platform_deal` d, `platform_reader` r
WHERE a.article_id = d.article_id AND d.reader_id = r.reader_id
      AND a.writer_id = w.writer_id AND r.reader_name = 'Fabian'
ORDER BY d.create_time DESC
LIMIT 3;
```

3.查询所有文章中付费人数最多的前3篇文章的名字，付费人数及总付费金额。

```
SELECT
  count(*)        dealcount,
  a.article_title atitle,
  w.writer_name   wname
FROM `platform_deal` d, `platform_article` a, `platform_writer` w
WHERE a.article_id = d.article_id AND w.writer_id = a.writer_id
GROUP BY atitle, wname
ORDER BY dealcount DESC
LIMIT 3;
```

4.平台所有的作者姓名(platform_writer表的writer_name字段)需要添加“w_”前缀，如“Joe”需要修改为“w_Joe”。

```
UPDATE `platform_writer`
SET writer_name = concat('w_', writer_name);
```

5.新创建的作者姓名仍是不带“w_”前缀的，因此需要在插入数据时自动为其添加“w_”前缀(用触发器解决，触发器的名称定义为“modifywritername”)。

```
CREATE TRIGGER modifywritername
BEFORE INSERT ON `platform_writer`
FOR EACH ROW
  BEGIN
    SET NEW.writer_name = concat('w_', NEW.writer_name);
  END;
```

6.查询每位作者的名称，该作者发表的文章总数，该作者的所有文章付费用户总数，按付费用户总数倒序排序。 

```
SELECT
  table1.wname,
  table1.articlecount,
  table2.readercount
FROM
  (SELECT
     count(*)      articlecount,
     w.writer_id   wid1,
     w.writer_name wname
   FROM `platform_article` a, `platform_writer` w
   WHERE a.writer_id = w.writer_id
   GROUP BY w.writer_id) table1
  JOIN
  (SELECT
     count(*)     readercount,
     a2.writer_id wid2
   FROM `platform_article` a2, `platform_deal` d2
   WHERE a2.article_id = d2.article_id
   GROUP BY a2.article_id) table2 ON table1.wid1 = table2.wid2;
```

7.创建一个试图article_writer， 包含文章的所有字段，文章的付费总额，文章作者的姓名和邮箱。

```
CREATE VIEW article_writer(aid, wid, atitle, acontent, acreatetime, atotalcount, wname, wemail) AS
  SELECT
    a.article_id,
    a.writer_id,
    a.article_title,
    a.content,
    a.create_time,
    count(
        d.deal_id),
    w.writer_name,
    w.writer_email
  FROM `platform_article` a, `platform_writer` w, `platform_deal` d
  WHERE a.article_id = d.article_id AND a.writer_id = w.writer_id
  GROUP BY a.article_id;
```

8.由于create_time是datetime格式，现在需要将其中的日期提取出来，查询每位读者每日的付费阅读总数和付费金额，结果集中包含读者ID，姓名，交易日期，当日付费阅读量，当日付费金额，并按照日期降序排序。 

```
SELECT
  r.reader_id,
  r.reader_name,
  date_format(d.create_time, '%Y-%m-%d') readdate,
  sum(d.deal_payment)                    totalpayment,
  count(*)                               readcount
FROM `platform_reader` r, `platform_deal` d
WHERE r.reader_id = d.reader_id
GROUP BY readdate, r.reader_id
ORDER BY readdate DESC;
```

