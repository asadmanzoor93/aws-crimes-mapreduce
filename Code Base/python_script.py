import pyspark
import findspark

import mysqlwrapper


def main():
    # Configure Spark and read csv
    findspark.init()
    sc=pyspark.SparkContext.getOrCreate()
    data = sc.textFile("https://s3.amazonaws.com/aws-bda-assign1/bda_data/crimes_sample.csv")
    data = data.map(lambda line: line.split(","))

    # fetch crimes as 6th column is crime
    crimes = data.filter(lambda x: x[5]).flatMap(lambda x: x[1].split())
    crimes = sc.parallelize(crimes)

    counts = crimes.map(lambda x: (x, 1)).reduceByKey(_ + _)
    reversed_map = counts.map(lambda (k, v): (v, k)).sortByKey(False)
    crimes_mapped = reversed_map.map(lambda (k, v): (v, k))

    conn = mysqlwrapper.get_connection()
    try:
        for key, value in crimes_mapped.items():
            insert_query = """INSERT INTO crimes (crime, count) VALUES('%s', '%i'); """ % (key, value)
            mysqlwrapper.execute_update_with_cnx(conn, insert_query)
            conn.commit()
            print('Crime Data Inserted !')
    except Exception as e:
        conn.rollback()
        mysqlwrapper.logging.exception(e)
        raise SystemExit(e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

