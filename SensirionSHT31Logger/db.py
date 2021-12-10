import mysql.connector
import importlib, time, os

import config
from error import RecordError

def connect():
	importlib.reload(config)
	return mysql.connector.connect( **config.conf['Logging']['mysql'] )

def record_logs(log_values: list, insert_sensor_ids : bool =False):
	"""
		Records values into the database.
		parameters:
			log_values: list of lists. The format is as follows [['sensor1_name', value1], ['sensor2_name', value2, ['sensor3_name', value3]]
	"""
	try:
		con = connect()
		cursor = con.cursor()
		
		if insert_sensor_ids:
			for row in log_values:
				sql = f"SELECT `sensor_id` FROM `sensors` WHERE `sensor_name`='{row[0]}'"
				cursor.execute(sql)
				results = cursor.fetchall()
				if len(results)==0:
					sql = f"INSERT INTO `sensors` (`sensor_name`, `sensor_friendlyname`) VALUES ('{row[0]}', '{row[0]}')"
					cursor.execute(sql)

		sql = "INSERT INTO `records`(`sensor_id`, `time`, `value`) VALUES "
		for row in log_values:
			sql += "((SELECT `sensor_id` FROM `sensors` WHERE `sensor_name`='{0:s}'), NOW(),{1:f}),".format(row[0], row[1])
		cursor.execute(sql[:-1])
		con.commit()
		cursor.close()
		con.close()
		
	except Exception as x:
		RecordError("Error recording logs into the database. device_name={0} log_values={1} Exception={2}".format(device_name, log_values, x))
		save_local(log_values)

def save_local(log_values: list):
	with open("./local_data.csv", "a") as f:
		for row in log_values:
			f.write(f"{row[0]},{round(time.time())},{row[1]}\n")

def send_local_data_to_server():
	local_file_name = "./local_data.csv"
	if not os.path.exists(local_file_name):
		return
	with open(local_file_name, "r") as f:
		lines = f.readlines()

	try:
		con = connect()
		cursor = con.cursor()
		sql = "INSERT INTO `records`(`sensor_id`, `time`, `value`) VALUES "
		for line in lines:
			row = line.strip().split(",")
			sql += "((SELECT `sensor_id` FROM `sensors` WHERE `sensor_name`='{0:s}'), FROM_UNIXTIME({1:d}),{2:f}),".format(row[0], int(row[1]), float(row[2]))
		
		cursor.execute(sql[:-1])
		con.commit()
		cursor.close()
		con.close()
		
		# Presumably everything got moved to the database
		os.remove(local_file_name)
		
	except Exception as x:
		print(x)
		# meh
		pass

if __name__ == "__main__":
	data = [['sensor1', 10], ['sensor2', 11]]
	#save_local(data)
	send_local_data_to_server()