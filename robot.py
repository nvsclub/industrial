from pymodbus.client.sync import ModbusTcpClient

TCP_IP_coils = '127.0.0.1'
TCP_PORT_coils = '5502'

TCP_IP_registers = '127.0.0.1'
TCP_PORT_registers = '5503'

client_coils = ModbusTcpClient(TCP_IP_coils, TCP_PORT_coils)
client_registers = ModbusTcpClient(TCP_IP_registers, TCP_PORT_registers)
	
def write_modbus_coil(memory_position, coil):
	client_coils.write_coil(memory_position, coil)
	
def write_modbus_multiple_coils(memory_position, list_coils):
	for coil in list_coils:
		client_coils.write_coil(memory_position, coil)
	
def write_modbus_register(memory_position, register):
	client_registers.write_register(memory_position, register)
	
def write_modbus_multiple_registers(memory_position, list_registers):
	for register in list_registers:
		client_registers.write_register(memory_position, register)
	
def read_modbus_coil(memory_position):
	reg = client_coils.read_coils(memory_position, 1)
	return reg.bits[0]

def read_modbus_multiple_coils(memory_position, no_coils):
	reg = client_coils.read_coils(memory_position, no_coils)
	return reg.bits[:no_coils]

def read_modbus_register(memory_position):
	reg = client_registers.read_holding_registers(memory_position, 1)
	return reg.registers

def read_modbus_multiple_registers(memory_position, no_registers):
	reg = client_registers.read_holding_registers(memory_position, no_registers)
	return reg.registers

client_coils.close()
client_registers.close()
