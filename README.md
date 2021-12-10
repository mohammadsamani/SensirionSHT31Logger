# Sensirion Thermometer Logger
 Logs temperature data from a Sensirion temperature/humidity [SHT31 Smart Gadget sensor](https://www.sensirion.com/fileadmin/user_upload/customers/sensirion/Dokumente/2_Humidity_Sensors/Sensirion_Humidity_Sensors_SHT3x_Smart-Gadget_User-Guide.pdf) through the Bluetooth interface, and log in a MySQL database.
 
 I run this on a [Raspberry Pi 3 Model B+](https://www.raspberrypi.com/products/raspberry-pi-3-model-b-plus/).
 
 To find out your device's MAC address and the UUID of the services, use ```bluetoothctl```. Read the [manual](https://www.makeuseof.com/manage-bluetooth-linux-with-bluetoothctl/) here.
 
 ## Run
 To run this, copy it onto your Raspberry Pi, or any other device with a Bluetooth adapter, navigate to this folder, make main.py executable, and execute it.
 ```
 sudo chmod +x main.py
 ./main.py
 ```
