from pymodbus.client.sync import ModbusTcpClient
import time
import threading
import psycopg2
import copy
from queue import Queue


# MODBUS MES <-> PLC constants
EnableStorage = 0
TakeObjectID = 0

Destino = [1, 6, 11, 16, 17, 18, 19, 20]

PX = [2, 4, 7, 9, 12, 14]
PY = [3, 5, 8, 10, 13, 15]

Ocupy = [1, 2, 3, 4, 5, 6]

Storage = 14

OcupyRobot = 7

EndRobot = 8

OcupyPusher = [9, 10, 11]


SOcupy = [0, 1, 2, 3, 4, 5]

SRotate = [6, 7, 8, 25, 26]

SMachine = [9, 10, 11, 12, 13, 14]

SStorage = 33

SOcupyRobot = 16

SEndRobot = 17

SBeltPusher = [27, 28, 29]

SRoller = [30, 31, 32]

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

######################################
########### SQL FUNCTIONS ############
######################################

def SqlLog():

    log = open("login.txt", 'r')
    info = log.read().splitlines()
    try:
        myconn=psycopg2.connect(host=info[0], user=info[1], password=info[2], dbname=info[3])
        myconn.autocommit = True
    except:
        print ("Error Connecting to SQL SERVER")
        return -1

    try:
        cur = myconn.cursor()
    except:
        print("ERROR CONNECTING TO SQL DATABASE")
        return -1

    log.close()
    return myconn

def SqlQuery(conn, cmd):

    cur = conn.cursor()
    try:
        cur.execute(cmd)
        result = cur.fetchall()
        return result
    except psycopg2.Error:
        return -1

def SqlQueryVarOne(conn, cmd, vars):

	cur = conn.cursor()
	try:
		cur.execute(cmd, vars)
		result = cur.fetchone()
		return result
	except psycopg2.Error:
		return -1

def SqlCount(conn,cmd):
    cur = conn.cursor()
    try:
        cur.execute(cmd)
        result = cur.rowcount
        return result
    except psycopg2.Error:
        return -1

def SqlCreate(conn, cmd):
    cur = conn.cursor()
    try:
        result = cur.execute(cmd)
        return result
    except psycopg2.Error:
        return -1

def SqlCreateVar(conn, cmd, vars):
    cur = conn.cursor()
    try:
        result = cur.execute(cmd,vars)
        return result
    except:
        return -1

def SqlClose(myconn):
    myconn.close()

# verifies if it is possible and takes the object out of the storage
# returns: False if the storage belt is ocupied
# True if everything worked
def handle_flag_storage_output(px, cell, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, pusher_id):
  if read_modbus_coil(SStorage) or client_coils.read_coils(EnableStorage, 1).bits[0]:
    return False
  # takes the object out of the storage
  write_modbus_register(TakeObjectID, px)
  write_modbus_coil(Storage, False)
  write_modbus_coil(EnableStorage, True)
  # refresh stacks for the rotators 
  if 0 <= cell:
    stack_0.put(cell)
  if 1 <= cell:
    stack_1.put(cell)
  if 2 <= cell:
    stack_2.put(cell)
  if 3 <= cell:
    stack_3.put(cell)
  if 4 <= cell:
    stack_4.put(cell)
  if 5 <= cell + pusher_id:
    stack_5.put(cell + pusher_id)
  if 6 <= cell + pusher_id:
    stack_6.put(cell + pusher_id)
  if 7 <= cell + pusher_id:
    stack_7.put(cell + pusher_id)
  return True

# checks if the object has reached the belt
# returns: False if the object hasnt reached yet
# True if it has reached
def handle_object_out(stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7):
  # waits for the object to be in the storage belt
  if read_modbus_coil(SStorage):
    # resets the storage variable
    write_modbus_coil(EnableStorage, False)
    write_modbus_coil(Storage, True)
    return True
  return False

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
  write_modbus_register(Destino[cell], 2)
  return

# handles the complete procedure of processing until the object leaves the second machine
# requires: (stacks) lists of object to pass by the cell sensor
# (cell) cell where processing happens,
# (modbus_user) flag to allow or disallow modbus interaction
# (x_machine_used) flag to tell if x machine is used
# (px) initial state, (py) final state of the object
# returns: True if everything was sucessfull and False if any error ocurred
def handle_request(sql_id, sql_conn, storage_flag, modbus_user, destino_flag_take, cell, first_machine_used, px1, py1, second_machine_used, px2, py2, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7):

  first_machine = cell * 2
  second_machine = cell * 2 + 1
  
  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  #print('ll')
  # verifies if the machines needed are free
  #print(handle_verify_machine(first_machine))
  #print((handle_verify_machine(first_machine) and first_machine_used and not second_machine_used))
  if (handle_verify_machine(first_machine) and first_machine_used and not second_machine_used) or (handle_verify_machine(second_machine) and second_machine_used and not first_machine_used) or (handle_verify_machine(first_machine) and first_machine_used and handle_verify_machine(second_machine) and second_machine_used):
    # reserves the machine
    if handle_verify_machine(first_machine) and first_machine_used:
      handle_reserve_machine(first_machine)
    if handle_verify_machine(second_machine) and second_machine_used:
      handle_reserve_machine(second_machine)

    #print('machines reserved', threading.current_thread())

    # tell the rotator to wait for an object
    handle_cell_usage(cell)

    # conceeds modbus_permission to another thread
    modbus_user.set()

    # wait for storage flag
    storage_flag.wait()
    storage_flag.clear()

    # if it is possible, flag the storage to output the object
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    if not px1 == 0:
      h = handle_flag_storage_output(px1, cell, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, 0)
    else:
      h = handle_flag_storage_output(px2, cell, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, 0)
    # conceeds modbus_permission to another thread
    modbus_user.set()
    while not h:
      time.sleep(0.1)
      # wait for permission to use modbus
      modbus_user.wait()
      # take the flag for himself
      modbus_user.clear()
      if not px1 == 0:
        h = handle_flag_storage_output(px1, cell, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, 0)
      else:
        h = handle_flag_storage_output(px2, cell, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, 0)
      # conceeds modbus_permission to another thread
      modbus_user.set()

    # waits for the object to be in the belt
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_object_out(stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7)
    # conceeds modbus_permission to another thread
    modbus_user.set()
    while not h:
      time.sleep(0.1)
      # wait for permission to use modbus
      modbus_user.wait()
      # take the flag for himself
      modbus_user.clear()
      h = handle_object_out(stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7)
      # conceeds modbus_permission to another thread
      modbus_user.set()
    
    #print('storage sucessfull', threading.current_thread())

    # free storage flag
    storage_flag.set()

    # conceeds modbus_permission to another thread
    modbus_user.set()
    time.sleep(0.1)

    # wait for the destino flags
    while not destino_flag_take.isSet():
      pass

    # reset destino flags
    destino_flag_take.clear()

    #print('destino sucessfull', threading.current_thread())

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

      #print('in the loop', threading.current_thread())
      
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

      #print('got into the machine', threading.current_thread())

      # checks if the first machine is used and executes
      if machine == first_machine and first_machine_used:
        initial_time = time.time()
        # wait for permission to use modbus
        modbus_user.wait()
        # take the flag for himself
        modbus_user.clear()
        handle_machine_processing(machine, px1, py1)
      elif machine == second_machine and second_machine_used:
        initial_time = time.time()
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
        print('dei 10 e 10')

      # conceeds modbus_permission to another thread
      modbus_user.set()
      time.sleep(0.1)

      #print('executed machine', threading.current_thread())

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

      #print('liberated machine')

      # wait for permission to use modbus
      modbus_user.wait()
      # take the flag for himself
      modbus_user.clear()
      # liberates the machine
      handle_liberate_machine(machine)
      # conceeds modbus_permission to another thread
      modbus_user.set()
      time.sleep(0.1)

      if (first_machine_used and machine == first_machine) or (second_machine_used and machine == second_machine):
        processing_time = time.time() - initial_time
        if first_machine_used:
          query = "SELECT total_time_seconds FROM machine_statistics WHERE id = %s"
          var = (first_machine+1,)
          res = SqlQueryVarOne(sql_conn,query,var)
          ##print(res)
          # update the database to signal order received
          query = "UPDATE machine_statistics SET total_time_seconds = %s WHERE id = %s"
          var = (res[0] + processing_time, first_machine+1)
          SqlCreateVar(sql_conn,query,var)
          # update the database to signal order received
          query = "SELECT type_P" + str(px1) + " FROM machine_statistics WHERE id = %s"
          var = (first_machine+1,)
          res = SqlQueryVarOne(sql_conn,query,var)
          #print(res)
          # update  
          query = "UPDATE machine_statistics SET type_P" + str(px1) + " = %s WHERE id = %s"
          var = (res[0] + 1, first_machine+1)
          SqlCreateVar(sql_conn,query,var)
        if second_machine_used:
          query = "SELECT total_time_seconds FROM machine_statistics WHERE id = %s"
          var = (second_machine+1,)
          res = SqlQueryVarOne(sql_conn,query,var)
          #print(res)
          # update the database to signal order received
          query = "UPDATE machine_statistics SET total_time_seconds = %s WHERE id = %s"
          var = (res[0] + processing_time, second_machine+1)
          SqlCreateVar(sql_conn,query,var)
          # update the database to signal order received
          query = "SELECT type_P" + str(px2) + " FROM machine_statistics WHERE id = %s"
          var = (second_machine+1,)
          res = SqlQueryVarOne(sql_conn,query,var)
          #print(res)
          # update  
          query = "UPDATE machine_statistics SET type_P" + str(px2) + " = %s WHERE id = %s"
          var = (res[0] + 1, second_machine+1)
          SqlCreateVar(sql_conn,query,var)

    if not py2 == 0:
      query = "SELECT quantity FROM warehouse WHERE type = %s"
      var = (py2,)
      res = SqlQueryVarOne(sql_conn,query,var)
      query = "UPDATE warehouse SET quantity = %s WHERE type = %s"
      var = (res[0] + 1, py2)
      SqlCreateVar(sql_conn,query,var)
    else:
      query = "SELECT quantity FROM warehouse WHERE type = %s"
      var = (py1,)
      res = SqlQueryVarOne(sql_conn,query,var)
      query = "UPDATE warehouse SET quantity = %s WHERE type = %s"
      var = (res[0] + 1, py1)
      SqlCreateVar(sql_conn,query,var)


  #print('ended thread', threading.current_thread())
  # conceeds modbus_permission to another thread
  modbus_user.set()

  # update the database to signal order received
  query = "UPDATE orders SET done_end = %s WHERE id = %s"
  var = (True, sql_id)
  SqlCreateVar(sql_conn,query,var)
  
# verify if robot is available
# returns: True if robot available, 
# False if robot is occupied
def robot_verify_availability():
  if read_modbus_coil(SOcupyRobot) and client_coils.read_coils(OcupyRobot):
    return False
  write_modbus_coil(OcupyRobot, True)
  return True

# reads robot flag to check if processing has ended
# returns: True if robot has ended his duty
# False if the robot hasnt finnished yet
def robot_check_ready():
  if read_modbus_coil(SEndRobot):
    return True
  return False

def robot_unload():
  write_modbus_coil(EndRobot, True)

def robot_finish():
  if read_modbus_coil(SRotate[3]) == True:
    write_modbus_coil(EndRobot, True)

def handle_robot(sql_id, sql_conn, storage_flag, modbus_user, destino_flag_take, destino_flag_give, px, py, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7):

  # if it is possible, flag the storage to output the object
  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  h = robot_verify_availability()
  # conceeds modbus_permission to another thread
  modbus_user.set()
  while not h:
    time.sleep(0.1)
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = robot_verify_availability()
    # conceeds modbus_permission to another thread
    modbus_user.set()

  storage_flag.wait()
  storage_flag.clear()

  # if it is possible, flag the storage to output the object
  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  h = handle_flag_storage_output(px, 3, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, 0)
  # conceeds modbus_permission to another thread
  modbus_user.set()
  while not h:
    time.sleep(0.1)
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_flag_storage_output(px, 3, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, 0)
    # conceeds modbus_permission to another thread
    modbus_user.set()

  # waits for the object to be in the belt
  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  h = handle_object_out(stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7)
  # conceeds modbus_permission to another thread
  modbus_user.set()
  while not h:
    time.sleep(0.1)
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_object_out(stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7)
    # conceeds modbus_permission to another thread
    modbus_user.set()

  # if it is possible, flag the storage to output the object
  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  h = handle_flag_storage_output(py, 3, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, 0)
  # conceeds modbus_permission to another thread
  modbus_user.set()
  while not h:
    time.sleep(0.1)
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_flag_storage_output(py, 3, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, 0)
    # conceeds modbus_permission to another thread
    modbus_user.set()

  # waits for the object to be in the belt
  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  h = handle_object_out(stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7)
  # conceeds modbus_permission to another thread
  modbus_user.set()
  while not h:
    time.sleep(0.1)
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_object_out(stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7)
    # conceeds modbus_permission to another thread
    modbus_user.set()

  
  storage_flag.set()

  # wait for the destino flags
  while not destino_flag_take.isSet():
    pass

  # reset destino flags
  destino_flag_take.clear()

  # waits for the robot to end processing
  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  h = robot_check_ready()
  # conceeds modbus_permission to another thread
  modbus_user.set()
  while not h:
    time.sleep(0.1)
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = robot_check_ready()
    # conceeds modbus_permission to another thread
    modbus_user.set()

  destino_flag_give.set()

  while destino_flag_give.isSet():
    pass


  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()

  robot_unload()



  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  h = robot_finish()
  # conceeds modbus_permission to another thread
  modbus_user.set()
  while not h:
    time.sleep(0.1)
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = robot_finish()
    # conceeds modbus_permission to another thread
    modbus_user.set()

    # update the database to signal order received
    query = "UPDATE orders SET done_end = %s WHERE id = %s"
    var = (True, sql_id)
    SqlCreateVar(sql_conn,query,var)


def handle_verify_pusher_ocupation(id):
  if client_coils.read_coils(OcupyPusher[id], 1).bits[0]:
    return False
  write_modbus_coil(OcupyPusher[id], True)
  return True

def handle_liberate_pusher(id):
  if client_coils.read_coils(OcupyPusher[id], 1).bits[0]:
    write_modbus_coil(OcupyPusher[id], False)
    return True
  return False

def handle_pusher(sql_id, sql_conn, storage_flag, modbus_user, destino_flag_take, destino_flag_push, pusher_id, px, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7):
  # verify if pusher is available
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  h = handle_verify_pusher_ocupation(pusher_id)
  # conceeds modbus_permission to another thread
  modbus_user.set()
  while not h:
    time.sleep(0.1)
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_verify_pusher_ocupation(pusher_id)
    # conceeds modbus_permission to another thread
    modbus_user.set()

  # handle storage
  storage_flag.wait()
  storage_flag.clear()

  # if it is possible, flag the storage to output the object
  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()


  h = handle_flag_storage_output(px, 4, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, pusher_id + 1)
  # conceeds modbus_permission to another thread
  modbus_user.set()
  while not h:
    time.sleep(0.1)
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_flag_storage_output(px, 4, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, pusher_id + 1)
    # conceeds modbus_permission to another thread
    modbus_user.set()

  # waits for the object to be in the belt
  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  h = handle_object_out(stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7)
  # conceeds modbus_permission to another thread
  modbus_user.set()
  while not h:
    time.sleep(0.1)
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_object_out(stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7)
    # conceeds modbus_permission to another thread
    modbus_user.set()

  storage_flag.set()


  # wait destino decision
  while not destino_flag_take.isSet():
    pass

  destino_flag_take.clear()

  # wait for pieces to be in the pusher
  destino_flag_push.clear()
  
  while not destino_flag_push.isSet():
    pass
  
  # liberate the pusher
  # wait for permission to use modbus
  modbus_user.wait()
  # take the flag for himself
  modbus_user.clear()
  handle_liberate_pusher(pusher_id)
  # conceeds modbus_permission to another thread
  modbus_user.set()

  # update the database to signal order received
  query = "UPDATE orders SET done_end = %s WHERE id = %s"
  var = (True, sql_id)
  SqlCreateVar(sql_conn,query,var)

  # order received
  query = "SELECT type_P" + str(px) + " FROM unload_statistics WHERE id = %s"
  var = (pusher_id,)
  res = SqlQueryVarOne(sql_conn,query,var)
  #print(res)
  # update  
  query = "UPDATE unload_statistics SET type_P" + str(px) + " = %s WHERE id = %s"
  var = (res[0] + 1, pusher_id)
  SqlCreateVar(sql_conn,query,var)




# puts all destinos to 2 as default
def initial_config():
  for destino in range(len(Destino)):
    write_modbus_register(Destino[destino], 2)
  return

def handle_check_machines():
  machine_states = []
  for machine_id in range(len(SMachine)):
    machine_states.append(client_coils.read_coils(Ocupy[machine_id], 1).bits[0])
  return machine_states

def handle_check_robot():
  if client_coils.read_coils(OcupyRobot, 1).bits[0] or read_modbus_coil(SOcupyRobot):
    return False
  return True

def handle_check_pushers():
  pusher_states = []
  for pusher_id in range(len(SBeltPusher)):
    pusher_states.append(client_coils.read_coils(OcupyPusher[pusher_id], 1).bits[0])
  return pusher_states
  


def handle_scheduler(modbus_user):
  # initializations
  previous_rotator_states = [False for _ in range(len(SRotate))]
  previous_machine_states = [False for _ in range(len(SMachine))]
  machine_locks = [False for _ in range(len(SMachine))]
  robot_lock = False
  pusher_locks = [False for _ in range(len(SBeltPusher))]

  destino_flag_take_1 = threading.Event()
  destino_flag_take_2 = threading.Event()
  destino_flag_take_3 = threading.Event()
  destino_flag_take_4 = threading.Event()
  destino_flag_take_5 = threading.Event()
  destino_flag_give = threading.Event()
  destino_flag_push_1 = threading.Event()
  destino_flag_push_2 = threading.Event()
  destino_flag_push_3 = threading.Event()
  

  storage_flag = threading.Event()
  storage_flag.set()


  stack_0 = Queue()
  stack_1 = Queue()
  stack_2 = Queue()
  stack_3 = Queue()
  stack_4 = Queue()
  stack_5 = Queue()
  stack_6 = Queue()
  stack_7 = Queue()

  th = threading.Thread(target=destino_manager, args=(storage_flag, modbus_user, destino_flag_take_1, destino_flag_take_2, destino_flag_take_3, destino_flag_take_4, destino_flag_take_5, destino_flag_give, destino_flag_push_1, destino_flag_push_2, destino_flag_push_3, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
  th.start()

  conn = SqlLog()

  i = "SET search_path TO ii"
  res = SqlQuery(conn,i)

  while True:
    # refresh which machines are free and which machines are ocupied
    modbus_user.wait()
    modbus_user.clear()
    machine_states = handle_check_machines()
    robot_state = handle_check_robot()
    pusher_states = handle_check_pushers()
    modbus_user.set()
    time.sleep(3)

    # always prioritize machine 1
    for machine_state in range(0,len(machine_states),2):
      cell = int(machine_state/2)

      # liberate machine locks
      if machine_locks[machine_state] and machine_states[machine_state]:
        machine_locks[machine_state] = False
      if machine_locks[machine_state + 1] and machine_states[machine_state + 1]:
        machine_locks[machine_state + 1] = False

      # check if there is any free cell
      if not machine_states[machine_state] and not machine_states[machine_state+1] and not machine_locks[machine_state] and not machine_locks[machine_state]:
        # select a order for the cell
        i = "SELECT id, cell, maq_1, px_1, py_1, maq_2, px_2, py_2 FROM orders WHERE cell = %s and done_start = %s"
        vars = (cell + 1, False)
        res = SqlQueryVarOne(conn,i,vars)
        #print(res)
        
        if res == None:
          continue

        # create thread to process the order
        if 0 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(res[0], conn, storage_flag, modbus_user, destino_flag_take_1, res[1]-1, res[2], res[3], res[4], res[5], res[6], res[7], stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
          th.start()
        if 1 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(res[0], conn, storage_flag, modbus_user, destino_flag_take_2, res[1]-1, res[2], res[3], res[4], res[5], res[6], res[7], stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
          th.start()
        if 2 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(res[0], conn, storage_flag, modbus_user, destino_flag_take_3, res[1]-1, res[2], res[3], res[4], res[5], res[6], res[7], stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
          th.start()

        # update the database to signal order received
        i = "UPDATE orders SET done_start = %s WHERE id = %s"
        var = (True, res[0])
        res = SqlCreateVar(conn,i,var)

        machine_locks[machine_state] = True
        machine_locks[machine_state+1] = True 
        
        break

      # check if there is any free machine 1
      elif not machine_states[machine_state] and not machine_locks[machine_state]:
        # select a order for the machine 1 of the cell
        i = "SELECT id, cell, maq_1, px_1, py_1, maq_2, px_2, py_2 FROM orders WHERE cell = %s and maq_1 = %s and done_start = %s"
        vars = (cell + 1, True, False)
        res = SqlQueryVarOne(conn,i,vars)

        if res == None:
          continue

        # create thread to process the order
        if 0 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(res[0], conn, storage_flag, modbus_user, destino_flag_take_1, res[1]-1, res[2], res[3], res[4], res[5], res[6], res[7], stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
          th.start()
        if 1 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(res[0], conn, storage_flag, modbus_user, destino_flag_take_2, res[1]-1, res[2], res[3], res[4], res[5], res[6], res[7], stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
          th.start()
        if 2 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(res[0], conn, storage_flag, modbus_user, destino_flag_take_3, res[1]-1, res[2], res[3], res[4], res[5], res[6], res[7], stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
          th.start()

        # update the database to signal order received
        i = "UPDATE orders SET done_start = %s WHERE id = %s"
        var = (True, res[0])
        res = SqlCreateVar(conn,i,var)

        machine_locks[machine_state] = True

        break

    time.sleep(3)


    if robot_lock and robot_state:
      robot_lock = False

    if robot_state and not robot_lock:
      cell = 3
      # select a order for the robot
      i = "SELECT px_1, py_1 FROM orders WHERE cell = %s and done_start = %s"
      vars = (cell + 1, False)
      res = SqlQueryVarOne(conn,i,vars)
      #print(res)

      if not res == None:

        # create thread to process the order
        th = threading.Thread(target=handle_robot, args=(res[0], conn, storage_flag, modbus_user, destino_flag_take_5, destino_flag_give, res[0], res[1], stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
        th.start()

        # update the database to signal order received
        i = "UPDATE orders SET done_start = %s WHERE id = %s"
        var = (True, res[0])
        res = SqlCreateVar(conn,i,var)

        robot_lock = True


    time.sleep(3)


    # check if there is any pusher
    for id in range(len(OcupyPusher)):
      
      # liberate pusher locks
      if pusher_locks[id] and pusher_states[id]:
        pusher_locks[id] = False

      if not pusher_states[id] and not pusher_locks[id]:
        cell = 4
        # select a order for the cell
        i = "SELECT id, cell, px_1, py_1 FROM orders WHERE cell = %s and done_start = %s"
        vars = (cell + 1, False)
        res = SqlQueryVarOne(conn,i,vars)
        #print(res)
        
        if res == None:
          continue

        # create thread to process the order
        if 1 == res[3]:
          # create thread to process the order
          th = threading.Thread(target=handle_pusher, args=(res[0], conn, storage_flag, modbus_user, destino_flag_take_5, destino_flag_push_1, res[3]-1, res[2], stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
          th.start()
        if 2 == res[3]:
          # create thread to process the order
          th = threading.Thread(target=handle_pusher, args=(res[0], conn, storage_flag, modbus_user, destino_flag_take_5, destino_flag_push_2, res[3]-1, res[2], stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
          th.start()
        if 3 == res[3]:
          # create thread to process the order
          th = threading.Thread(target=handle_pusher, args=(res[0], conn, storage_flag, modbus_user, destino_flag_take_5, destino_flag_push_3, res[3]-1, res[2], stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7))
          th.start()

        # update the database to signal order received
        i = "UPDATE orders SET done_start = %s WHERE id = %s"
        var = (True, res[0])
        SqlCreateVar(conn,i,var)

        pusher_locks[id] = True

        break


  SqlClose(conn)

# redirects the piece in the rotative belt into the cell
# requires: (rotative_id) id of the rotative belt to handle
# (destino_array) array of the pieces to pass by that belt
# returns: True if the information was passed to the belt
# False if the piece destination did not match the cell
def handle_destino(rotative_id, rotate, to_read):
  # check if the piece if for himself
  if rotate:
    write_modbus_register(Destino[rotative_id], 0)
    while read_modbus_coil(to_read):
      pass
    write_modbus_register(Destino[rotative_id], 2)
    return True
  else:
    write_modbus_register(Destino[rotative_id], 1)
    while read_modbus_coil(to_read):
      pass
    write_modbus_register(Destino[rotative_id], 2)
  return False

# destino manager
def destino_manager(storage_flag, modbus_user, destino_flag_take_1, destino_flag_take_2, destino_flag_take_3, destino_flag_take_4, destino_flag_take_5, destino_flag_give, destino_flag_push_1, destino_flag_push_2, destino_flag_push_3, stack_0, stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7):
  sensor_usage = [False for _ in range(len(SRotate))]
  for _ in range(len(SBeltPusher)):
    sensor_usage.append(False)
  while 1:
    modbus_user.wait()
    modbus_user.clear()
    rotate_sensors = []
    for i in range(len(SRotate)):
      rotate_sensors.append(read_modbus_coil(SRotate[i]))
    for i in range(len(SBeltPusher)):
      rotate_sensors.append(read_modbus_coil(SBeltPusher[i]))
    modbus_user.set()
    time.sleep(0.3)

    #print(list(stack_0.queue), list(stack_1.queue), list(stack_2.queue), list(stack_3.queue), list(stack_4.queue), list(stack_5.queue), list(stack_6.queue), list(stack_7.queue))

    for rotate_sensor in range(len(rotate_sensors)):
      if not rotate_sensors[rotate_sensor]:
        sensor_usage[rotate_sensor] = False

    modbus_user.wait()
    modbus_user.clear()

    for rotate_sensor in range(len(rotate_sensors)):
      if rotate_sensors[rotate_sensor] and not sensor_usage[rotate_sensor]:
        sensor_usage[rotate_sensor] = True
        if rotate_sensor == 0:
          if stack_0.get() == 0:
            handle_destino(rotate_sensor, True, SRotate[0])
            destino_flag_take_1.set()
          else:
            handle_destino(rotate_sensor, False, SRotate[0])
        elif rotate_sensor == 1:
          if stack_1.get() == 1:
            handle_destino(rotate_sensor, True, SRotate[1])
            destino_flag_take_2.set()
          else:
            handle_destino(rotate_sensor, False, SRotate[1])
        elif rotate_sensor == 2:
          if stack_2.get() == 2:
            handle_destino(rotate_sensor, True, SRotate[2])
            destino_flag_take_3.set()
          else:
            handle_destino(rotate_sensor, False, SRotate[2])
        elif rotate_sensor == 3:
          if stack_3.get() == 3:
            handle_destino(rotate_sensor, True, SRotate[3])
            destino_flag_take_4.set()
          else:
            handle_destino(rotate_sensor, False, SRotate[3])
        elif rotate_sensor == 4:
          if stack_4.get() == 4:
            handle_destino(rotate_sensor, True, SRotate[4])
            destino_flag_take_5.set()
          else:
            handle_destino(rotate_sensor, False, SRotate[4])
        elif rotate_sensor == 5:
          if stack_5.get() == 5:
            handle_destino(rotate_sensor, True, SBeltPusher[0])
            destino_flag_push_1.set()
          else:
            handle_destino(rotate_sensor, False, SBeltPusher[0])
        elif rotate_sensor == 6:
          if stack_6.get() == 6:
            handle_destino(rotate_sensor, True, SBeltPusher[1])
            destino_flag_push_2.set()
          else:
            handle_destino(rotate_sensor, False, SBeltPusher[1])
        elif rotate_sensor == 7:
          if stack_7.get() == 7:
            handle_destino(rotate_sensor, True, SBeltPusher[2])
            destino_flag_push_3.set()
          else:
            handle_destino(rotate_sensor, False, SBeltPusher[2])

    if destino_flag_give.isSet():
      if stack_4.empty():
        destino_flag_give.clear()
        handle_destino(3, False, SRotate[3])

    modbus_user.set()

# tests for handle_request
# threads
initial_config()

modbus_user = threading.Event()
modbus_user.set()

handle_scheduler(modbus_user)

client_output.close()
client_coils.close()
