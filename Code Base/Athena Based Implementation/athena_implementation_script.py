import boto
from pyathena import connect

import mysqlwrapper


def select_data(query):
	data_set = []	
	con = boto.connect_s3()
	connection = connect(s3_staging_dir='s3://aws-bda-assign1/bda_data/')
	cursor = connection.cursor()
	cursor.execute(query)
	if cursor.has_result_set:		
		data = cursor.description
		columns = get_columns(data)		
		data_set = convert_result_dict(cursor.fetchall(),columns)		

	cursor.close()
	connection.close()

	return data_set


def get_columns(cur_desc):
	cols = []
	for row in cur_desc:
		cols.append(row[0])
	return cols	


def convert_result_dict(result, cols):
	rows = []
	for row in result:
		row_dict = {}
		for index, col in enumerate(cols):
			row_dict[col] = row[index]
		rows.append(row_dict)		
	return rows	


def main():
	result = select_data("select * from bda_assign1.crimes")

	counts = {}
	for item in result:
		if item['primary_type'] and item['primary_type']!='primary_type':
			word = item['primary_type'].lower()
			counts[word] = counts.get(word, 0) + 1

	conn = mysqlwrapper.get_connection()

	try:
		for key, value in counts.items():
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

