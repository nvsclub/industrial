from pymodbus.client.sync import ModbusTcpClient
import time
import threading

# MODBUS MES <-> PLC constants
EnableStorage = 0
TakeObjectID = 0

Destino = [1, 6, 11]

PX = [2, 4, 7, 9, 12, 14]
PY = [3, 5, 8, 10, 13, 15]

Ocupy = [1, 2, 3, 4, 5, 6]

SRotate = [6, 7, 8]

SStorage = 15

SMachine = [9, 10, 11, 12, 13, 14]

SOcupy = [0, 1, 2, 3, 4, 5]

TCP_IP_coils = '127.0.0.1'
TCP_PORT_coils = '5503'

client_coils = ModbusTcpClient(TCP_IP_coils, TCP_PORT_coils)
client_output = ModbusTcpClient(TCP_IP_coils, '5504')

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


def handle_choose_object(px):
  # choose object
  write_modbus_register(TakeObjectID, px)

def handle_flag_storage_output():
  if read_modbus_coil(SStorage):
    return False
  # takes the object out of the storage
  write_modbus_coil(EnableStorage, True)
  return True

def handle_object_out():
  # waits for the object to be in the storage belt
  if read_modbus_coil(SStorage):
    # resets the storage variable
    write_modbus_coil(EnableStorage, False)
    return True
  return False

'''# takes an object out of the storage into the belt
# requires: (px) object to be taken out
# returns: true if everything worked
def handle_storage(px):
  # choose object
  write_modbus_register(TakeObjectID, px)
  # checks and waits for storage belt to be empty
  while read_modbus_coil(SStorage):
    pass
  # takes the object out of the storage
  write_modbus_coil(EnableStorage, True)
  # waits for the object to be in the storage belt
  while not read_modbus_coil(SStorage):
    pass
  # waits for the object to be out of the storage belt
  while read_modbus_coil(SStorage):
    pass
  # resets the storage variable
  write_modbus_coil(EnableStorage, False)
  return True'''

# verifies if the machine is already being used by another thread
# requires: (machine_id) machine to be verified
# returns: True if machine is free, False if machine is in use
def handle_verify_machine(machine_id):
  # if occupied
  if read_modbus_coil(SOcupy[machine_id]) or client_coils.read_coils(Ocupy[machine_id], 1).bits[0]:
    return False
  # if free
  return True

# reserves the machine to be used and locks it
# requires: (machine_id) machine to be locked
# returns: True if machine is locked, False if machine was already in use
def handle_reserve_machine(machine_id):
  # if already occupied return error
  if read_modbus_coil(SOcupy[machine_id]) or client_coils.read_coils(Ocupy[machine_id], 1).bits[0]:
    return False
  # if it is free reserve it
  write_modbus_coil(Ocupy[machine_id], True)
  return True

# resets the machine state freeing the ocupied state and locking the processing to wait for a request
# requires: (machine_id) machine to be freed
# returns: True if everything was done sucessfully
def handle_liberate_machine(machine_id):
  # liberate the machine
  write_modbus_coil(Ocupy[machine_id], False)
  write_modbus_register(PX[machine_id], 0)
  write_modbus_register(PY[machine_id], 0)
  return True  

# redirects the piece in the rotative belt into the cell
# requires: (rotative_id) id of the rotative belt to handle
# (destino_array) array of the pieces to pass by that belt
# returns: True if the information was passed to the belt
# False if the piece destination did not match the cell
def handle_destino(rotative_id, destino_array):
  # check if the piece if for himself
  if rotative_id == destino_array[0]:
    write_modbus_register(Destino[rotative_id], 2)
    while read_modbus_coil(SRotate[rotative_id]):
      pass
    write_modbus_register(Destino[rotative_id], 1)
  else:
    write_modbus_register(Destino[rotative_id], 1)
    while read_modbus_coil(SRotate[rotative_id]):
      pass
    write_modbus_register(Destino[rotative_id], 0)
  return True

# tells the machine which processing steps it should make
# required: (machine_id) machine that will be proessing
# (px) initial state, (py) final state of the object
# returns: True if everything was sucessfull
def handle_machine_processing(machine_id, px, py):
  write_modbus_register(PX[machine_id], px)
  write_modbus_register(PY[machine_id], py)
  return True

# sets the rotator belt to wait when a piece arrives at him
# required: (cell) cell of the rotator to alert
def handle_cell_usage(cell):
  write_modbus_register(Destino[cell], 0)
  return

# handles the complete procedure of processing until the object leaves the second machine
# requires: (cell) cell where processing happens,
# (modbus_user) flag to allow or disallow modbus interaction
# (x_machine_used) flag to tell if x machine is used
# (px) initial state, (py) final state of the object
# (object_entries) list of objects on the top belts that are going to pass by the cell
# returns: True if everything was sucessfull and False if any error ocurred
def handle_request(modbus_user, cell, first_machine_used, px1, py1, second_machine_used, px2, py2, object_entries):
  first_machine = cell * 2
  second_machine = cell * 2 + 1

  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  # verifies if the machines needed are free
  #print(handle_verify_machine(first_machine))
  #print((handle_verify_machine(first_machine) and first_machine_used and not second_machine_used))
  if (handle_verify_machine(first_machine) and first_machine_used and not second_machine_used) or (handle_verify_machine(second_machine) and second_machine_used and not first_machine_used) or (handle_verify_machine(first_machine) and first_machine_used and handle_verify_machine(second_machine) and second_machine_used):
    # reserves the machine
    if handle_verify_machine(first_machine) and first_machine_used:
      handle_reserve_machine(first_machine)
    if handle_verify_machine(second_machine) and second_machine_used:
      handle_reserve_machine(second_machine)

    print('machines reserved', threading.current_thread())

    # tell the rotator to wait for an object
    handle_cell_usage(cell)

    # conceeds modbus_permission to another thread
    modbus_user.set()

    # tell the storage the object to process
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    handle_choose_object(px1)
    # conceeds modbus_permission to another thread
    modbus_user.set()


    # if it is possible, flag the storage to output the object
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_flag_storage_output()
    # conceeds modbus_permission to another thread
    modbus_user.set()
    while not h:
      # wait for permission to use modbus
      modbus_user.wait()
      # take the flag for himself
      modbus_user.clear()
      h = handle_flag_storage_output()
      # conceeds modbus_permission to another thread
      modbus_user.set()

    # checks if the object is out
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_object_out()
    # conceeds modbus_permission to another thread
    modbus_user.set()
    while not h:
      # wait for permission to use modbus
      modbus_user.wait()
      # take the flag for himself
      modbus_user.clear()
      h = handle_object_out()
      # conceeds modbus_permission to another thread
      modbus_user.set()
    

    print('storage sucessfull', threading.current_thread())

    # conceeds modbus_permission to another thread
    modbus_user.set()
    time.sleep(0.1)

    # manage rotating belts and gathers the object into the cell
    while True:
      # wait for permission to use modbus
      modbus_user.wait()
      # take the flag for himself
      modbus_user.clear()
      # read the sensor
      sensor_read = read_modbus_coil(SRotate[cell])
      # conceeds modbus_permission to another thread
      modbus_user.set()
      time.sleep(0.1)
      # get out of the cycle when the right object arrives at the right cell
      if sensor_read and object_entries[0] == cell:
        break

      # take an object out of the list everytime it passed thought the sensor
      if sensor_read and not object_entries[0] == cell:
        object_entries.pop(0)

      # wait for it to leave the sensor
      while sensor_read:
        # wait for permission to use modbus
        modbus_user.wait()
        # take the flag for himself
        modbus_user.clear()
        # read the sensor
        sensor_read = read_modbus_coil(SRotate[cell])
        # conceeds modbus_permission to another thread
        modbus_user.set()
        time.sleep(0.1)
      pass

    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()

    # gather the object to the cell
    handle_destino(cell, object_entries)

    # conceeds modbus_permission to another thread
    modbus_user.set()

    print('destino sucessfull', threading.current_thread())

    for machine in [first_machine, second_machine]:
      # wait for permission to use modbus
      modbus_user.wait()
      # take the flag for himself
      modbus_user.clear()
      # waits for the machine to be empty
      # refresh the state of the machine sensor
      sensor_machine = read_modbus_coil(SMachine[machine])
      # conceeds modbus_permission to another thread
      modbus_user.set()

      print('in the loop', threading.current_thread())
      
      # waits for object to reach the machine sensor
      while not sensor_machine:
        # wait for permission to use modbus
        modbus_user.wait()
        # take the flag for himself
        modbus_user.clear()
        # refresh the state of the machine sensor
        sensor_machine = read_modbus_coil(SMachine[machine])
        # conceeds modbus_permission to another thread
        modbus_user.set()
        time.sleep(0.1)

      print('got into the machine', threading.current_thread())

      # checks if the first machine is used and executes
      if machine == first_machine and first_machine_used:
        # wait for permission to use modbus
        modbus_user.wait()
        # take the flag for himself
        modbus_user.clear()
        handle_machine_processing(machine, px1, py1)
      elif machine == second_machine and second_machine_used:
        # wait for permission to use modbus
        modbus_user.wait()
        # take the flag for himself
        modbus_user.clear()
        handle_machine_processing(machine, px2, py2)
      else:
        # wait for permission to use modbus
        modbus_user.wait()
        # take the flag for himself
        modbus_user.clear()
        handle_machine_processing(machine, 10, 10)

      # conceeds modbus_permission to another thread
      modbus_user.set()
      time.sleep(0.1)

      print('executed machine', threading.current_thread())

      # checks if the object left the machine and liberates the machine
      # wait for permission to use modbus
      modbus_user.wait()
      # take the flag for himself
      modbus_user.clear()
      # refresh the state of the machine sensor
      sensor_machine = read_modbus_coil(SMachine[machine])
      # conceeds modbus_permission to another thread
      modbus_user.set()
      time.sleep(0.1)
      while sensor_machine:
        # wait for permission to use modbus
        modbus_user.wait()
        # take the flag for himself
        modbus_user.clear()
        # refresh the state of the machine sensor
        sensor_machine = read_modbus_coil(SMachine[machine])
        # conceeds modbus_permission to another thread
        modbus_user.set()
        time.sleep(0.1)

      # wait for permission to use modbus
      modbus_user.wait()
      # take the flag for himself
      modbus_user.clear()
      # liberates the machine
      handle_liberate_machine(machine)
      # conceeds modbus_permission to another thread
      modbus_user.set()
      time.sleep(0.1)

  print('ended thread', threading.current_thread())
  # conceeds modbus_permission to another thread
  modbus_user.set()

# puts all destinos to 1 as default
def initial_config():
  for destino in range(len(Destino)):
    write_modbus_register(Destino[destino], 1)
  return


# tests for handle_request
# threads
initial_config()

modbus_user = threading.Event()
modbus_user.set()

th1 = threading.Thread(target=handle_request, args=(modbus_user, 0, True, 1, 7, True, 7, 8, [0]))
th1.start()

time.sleep(5)

th2 = threading.Thread(target=handle_request, args=(modbus_user, 1, True, 1, 7, True, 7, 8, [1]))
th2.start()

time.sleep(5)

th3 = threading.Thread(target=handle_request, args=(modbus_user, 2, True, 2, 8, True, 8, 9, [2]))
th3.start()

time.sleep(50)

th4 = threading.Thread(target=handle_request, args=(modbus_user, 0, True, 1, 3, False, 10, 10, [0]))
th4.start()

  
th1.join()
th2.join()
th3.join()
th4.join()


client_output.close()
client_coils.close()
