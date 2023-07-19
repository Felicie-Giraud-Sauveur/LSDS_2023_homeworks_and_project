---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.14.5
  kernelspec:
    display_name: Bash
    language: bash
    name: bash
---

# Prepare environment

You must prepare your environment __if you have not done so in the previous exercises__ in order to do this homework. Follow the steps below.


-----
1. Run the next cell, and verify that you are logged as you, and not as someone else

```bash
echo "You are ${USERNAME:-nobody}"
```

-----
2. Run the code below and verify the existance of your database. Execute step 3 if you have a database.\
Otherwise go to step 6, if it shows an empty list as shown below:

```
    +-----------------+
    |  database_name  |
    +-----------------+
    +-----------------+
```

```bash
beeline -u "${HIVE_JDBC_URL}" -e "SHOW DATABASES LIKE '${USERNAME}';"
```

```bash
beeline -u "${HIVE_JDBC_URL}" -e "SHOW DATABASES LIKE '${USERNAME}';"
```

-----
3. Review the content of your database if you have one.

```bash
beeline -u "${HIVE_JDBC_URL}" -e "SHOW TABLES IN ${USERNAME};"
```

-----
4. Drop your database after having reviewed its content in step 3, and __you are ok losing its content__.

```bash
beeline -u "${HIVE_JDBC_URL}" -e "DROP DATABASE IF EXISTS ${USERNAME} CASCADE;"
```

-----
5. Verify that you the database is gone

```bash
beeline -u "${HIVE_JDBC_URL}" -e "SHOW DATABASES LIKE '${USERNAME}';"
```

-----
6. Run the remaining cells to reconstruct your hive folder and reconfigure ACL permissions on HDFS

```bash
hdfs dfs -ls "/user/${USERNAME}"
```

```bash
hdfs dfs -rm -r -f -skipTrash "/user/${USERNAME:-nobody}/hive"
hdfs dfs -rm -r -f -skipTrash "/user/${USERNAME:-nobody}/.Trash"
```

```bash
hdfs dfs -mkdir -p                                /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl    -m group::r-x                /user/${USERNAME:-nobody}
hdfs dfs -setfacl    -m other::---                /user/${USERNAME:-nobody}
hdfs dfs -setfacl    -m default:group::r-x        /user/${USERNAME:-nobody}
hdfs dfs -setfacl    -m default:other::---        /user/${USERNAME:-nobody}
hdfs dfs -setfacl -R -m group::r-x                /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl -R -m other::---                /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl -R -m default:group::r-x        /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl -R -m default:other::---        /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl    -m user:hive:rwx             /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl    -m default:user:hive:rwx     /user/${USERNAME:-nobody}/hive
```

-----
7. Recreate the __external__ tables `sbb_orc` and `sbb_05_11_2018` which you will need in the homework.

```bash
beeline -u "${HIVE_JDBC_URL}" --silent=true -e "
CREATE DATABASE IF NOT EXISTS ${USERNAME:-nobody} LOCATION '/user/${USERNAME:-nobody}/hive';

USE ${USERNAME:-nobody};

DROP TABLE IF EXISTS ${USERNAME:-nobody}.sbb_orc;

CREATE EXTERNAL TABLE ${USERNAME:-nobody}.sbb_orc(
        betriebstag STRING,
        fahrt_bezichner STRING,
        betreiber_id STRING,
        betreiber_abk STRING,
        betreiber_name STRING,
        produkt_id STRING,
        linien_id STRING,
        linien_TEXT STRING,
        umlauf_id STRING,
        verkehrsmittel_text STRING,
        zusatzfahrt_tf STRING,
        faellt_aus_tf STRING,
        bpuic STRING,
        haltestellen_name STRING,
        ankunftszeit STRING,
        an_prognose STRING,
        an_prognose_status STRING,
        abfahrtszeit STRING,
        ab_prognose STRING,
        ab_prognose_status STRING,
        durchfahrt_tf STRING
    )
    PARTITIONED BY (year INTEGER, month INTEGER)
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/istdaten';
    
    MSCK REPAIR TABLE sbb_orc;
    "
```

```bash

```
