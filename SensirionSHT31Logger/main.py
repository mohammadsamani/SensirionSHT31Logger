# Author: Mohammad Samani
# Date:   1.12.2021
# Place:  Basel, Switzerland
import time, platform, struct, binascii, asyncio
from config import conf
from error import RecordError
from bleak import BleakClient
from bleak.backends.winrt.client import BleakClientWinRT
import db

ADDRESS = conf['mac_address']
TEMP_UUID = conf['Sensors']['temperature']['uuid']
HUM_UUID = conf['Sensors']['humidity']['uuid']

async def getdata_windows():
	"""
		This function runs only on Windows. I have only tested this on Windows 10.
	"""
	async with BleakClientWinRT(ADDRESS, address_type="random", timeout=100) as client:
		try:
			tmp_bytearr = await client.read_gatt_char(TEMP_UUID)
			temperature = struct.unpack('<f',tmp_bytearr)[0]
			
			hum_bytearr = await client.read_gatt_char(HUM_UUID)
			humidity = struct.unpack('<f',hum_bytearr)[0]
			print(temperature, humidity)
			
			return temperature, humidity
		except Exception as ex:
			print(ex)
			RecordError(f"{ex}")

async def getdata_linux():
	"""
		This function runs only on Linux. I have only tested this on Raspbian running on a Raspberry Pi.
	"""
	async with BleakClient(ADDRESS, address_type="random", timeout=100) as client:
		try:
			tmp_bytearr = await client.read_gatt_char(TEMP_UUID)
			temperature = struct.unpack('<f',tmp_bytearr)[0]
			
			hum_bytearr = await client.read_gatt_char(HUM_UUID)
			humidity = struct.unpack('<f',hum_bytearr)[0]
			
			return temperature, humidity
		except Exception as ex:
			print(ex)
			RecordError(f"{ex}")


system = platform.system()
if system not in ["Windows", "Linux"]:
	print("You need either Windows or Linux to run this.")
	exit(1)

while True:
	time.sleep(conf['Logging']['SleepInLoop'])
	if system == "Windows":
		temperature, humidity = asyncio.run(getdata_windows())
	if system == "Linux":
		temperature, humidity = asyncio.run(getdata_linux())
	db.record_logs(conf['Machine'],[[conf['Sensors']['humidity']['name'], humidity],[conf['Sensors']['temperature']['name'], temperature]])
	db.send_local_data_to_server()