import serial
conf = {
	"ReloadPeriod":		60,
	"Logging": {
		"SleepInLoop":		60,
		"mysql": {
			"host":			"phys-dots-data.physik.unibas.ch",
			"user":			"logger",
			"password":		"********",
			"database":		"logs",
		}
	},
	"Machine":		"sensirion",
	"Sensors":	{
			"humidity": {"name": "spin_lab_humidity", "uuid": "00001235-b38d-4985-720e-0f993a68ee41"},
			"temperature": {"name": "spin_lab_temperature", "uuid": "00002235-b38d-4985-720e-0f993a68ee41"}
	},
	"Errorfile":		"./error.log",
	"mac_address":		"df:76:3e:07:c9:ee", # This is the sensor's MAC address.
}
