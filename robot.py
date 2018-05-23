from pymodbus.client.sync import ModbusTcpClient
import time
import threading
import psycopg2
import copy
from queue import Queue


# MODBUS MES <-> PLC constants
EnableStorage = 0
TakeObjectID = 0

Destino = [1, 6, 11, 16]

PX = [2, 4, 7, 9, 12, 14]
PY = [3, 5, 8, 10, 13, 15]

Ocupy = [1, 2, 3, 4, 5, 6]


SRotate = [6, 7, 8, 16]

SStorage = 15

SMachine = [9, 10, 11, 12, 13, 14]

SOcupy = [0, 1, 2, 3, 4, 5]

OcupyRobot = 7

SOcupyRobot = 16

EndRobot = 8

SEndRobot = 17

TCP_IP_coils = '127.0.0.1'
TCP_PORT_coils = '5503'

client_coils = ModbusTcpClient(TCP_IP_coils, TCP_PORT_coils)
client_output = ModbusTcpClient(TCP_IP_coils, '5504')

#### MODBUS

def write_modbus_coil(memory_position, coil):
	client_coils.write_coil(memory_position, coil)
	
def write_modbus_multiple_coils(memory_position, list_coils):
	i = memory_position
	for coil in list_coils:
		client_coils.write_coil(i, coil)
		i += 1
	
def write_modbus_register(memory_position, register):
	client_coils.write_register(memory_position, register)
	
def write_modbus_multiple_registers(memory_position, list_registers):
	i = memory_position
	for register in list_registers:
		client_coils.write_register(i, register)
		i += 1
	
def read_modbus_coil(memory_position):
	reg = client_output.read_coils(memory_position, 1)
	return reg.bits[0]

def read_modbus_multiple_coils(memory_position, no_coils):
	reg = client_output.read_coils(memory_position, no_coils)
	return reg.bits[:no_coils]

def read_modbus_register(memory_position):
	reg = client_output.read_holding_registers(memory_position, 1)
	return reg.registers

def read_modbus_multiple_registers(memory_position, no_registers):
	reg = client_output.read_holding_registers(memory_position, no_registers)
	return reg.registers

def initial_config():
  for destino in range(len(Destino)-1):
    write_modbus_register(Destino[destino], 1)
  write_modbus_register(Destino[3], 2)
  return

# verify if robot is available
# returns: True if robot available, 
# False if robot is occupied
def robot_verify_availability():
  if read_modbus_coil(SOcupyRobot) and client_coils.read_coils(OcupyRobot):
    return False
  write_modbus_coil(OcupyRobot, True)
  return True

# verifies if it is possible and takes the object out of the storage
# returns: False if the storage belt is ocupied
# True if everything worked
def handle_flag_storage_output(px):
  if read_modbus_coil(SStorage) or client_coils.read_coils(EnableStorage, 1).bits[0]:
    return False
  # takes the object out of the storage
  write_modbus_register(TakeObjectID, px)
  write_modbus_coil(EnableStorage, True)
  return True

# checks if the object has reached the belt
# returns: False if the object hasnt reached yet
# True if it has reached
def handle_object_out():
  # waits for the object to be in the storage belt
  if read_modbus_coil(SStorage):
    # resets the storage variable
    write_modbus_coil(EnableStorage, False)
    return True
  return False

# reads robot flag to check if processing has ended
# returns: True if robot has ended his duty
# False if the robot hasnt finnished yet
def robot_check_ready():
  if read_modbus_coil(SEndRobot):
    return True
  return False



def robot_unload():
  # ta a lixar aqui mas nao interessa
  if client_coils.read_holding_registers(Destino[3], 1) == 2:
    write_modbus_register(Destino[3], 1)
    write_modbus_coil(EndRobot, True)
    return True
  return False



initial_config()

p1 = 1
p2 = 2

while not robot_verify_availability():
  pass

print('robot available')

while not handle_flag_storage_output(p1):
  pass

print('storage init')
while not handle_object_out():
  pass

print('storage end')
while not handle_flag_storage_output(p2):
  pass

print('storage init')
while not handle_object_out():
  pass

print('storage end')
while not robot_check_ready():
  pass

print('robot unloading')
while not robot_unload():
  pass

'''print('executed')

write_modbus_coil(7, True)
print(client_coils.read_coils(7, 1).bits[0])
'''
client_output.close()
client_coils.close()

