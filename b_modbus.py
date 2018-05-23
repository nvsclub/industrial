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
def handle_request(stack, modbus_user, destino_flag_take, destino_flag_move, cell, first_machine_used, px1, py1, second_machine_used, px2, py2):
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

    print('machines reserved', threading.current_thread())

    # tell the rotator to wait for an object
    handle_cell_usage(cell)

    # conceeds modbus_permission to another thread
    modbus_user.set()

    # if it is possible, flag the storage to output the object
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    if not px1 == 0:
      h = handle_flag_storage_output(px1)
    else:
      h = handle_flag_storage_output(px2)
    # conceeds modbus_permission to another thread
    modbus_user.set()
    while not h:
      time.sleep(0.1)
      # wait for permission to use modbus
      modbus_user.wait()
      # take the flag for himself
      modbus_user.clear()
      if not px1 == 0:
        h = handle_flag_storage_output(px1)
      else:
        h = handle_flag_storage_output(px2)
      # conceeds modbus_permission to another thread
      modbus_user.set()

    # waits for the object to be in the belt
    # wait for permission to use modbus
    modbus_user.wait()
    # take the flag for himself
    modbus_user.clear()
    h = handle_object_out()
    # conceeds modbus_permission to another thread
    modbus_user.set()
    while not h:
      time.sleep(0.1)
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

    # wait for the destino flags
    while not destino_flag_take.isSet():
      pass

    # reset destino flags
    destino_flag_take.clear()

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
    write_modbus_register(Destino[destino], 2)
  return


def handle_check_machines():
  machine_states = []
  for machine_id in range(len(SMachine)):
    machine_states.append(client_coils.read_coils(Ocupy[machine_id], 1).bits[0])
  return machine_states

def handle_scheduler(modbus_user):
  # initializations
  previous_rotator_states = [False for _ in range(len(SRotate))]
  previous_machine_states = [False for _ in range(len(SMachine))]
  machine_locks = [False for _ in range(len(SMachine))]

  destino_flag_take_1 = threading.Event()
  destino_flag_take_2 = threading.Event()
  destino_flag_take_3 = threading.Event()
  destino_flag_move_1 = threading.Event()
  destino_flag_move_2 = threading.Event()
  destino_flag_move_3 = threading.Event()


  stack_0 = Queue()
  stack_1 = Queue()
  stack_2 = Queue()

  th = threading.Thread(target=destino_manager, args=(modbus_user, destino_flag_take_1, destino_flag_take_2, destino_flag_take_3, destino_flag_move_1, destino_flag_move_2, destino_flag_move_3, stack_0, stack_1, stack_2))
  th.start()

  conn = SqlLog()

  i = "SET search_path TO ii"
  res = SqlQuery(conn,i)

  print(conn)

  while True:
    # refresh which machines are free and which machines are ocupied
    modbus_user.wait()
    modbus_user.clear()
    machine_states = handle_check_machines()
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
        i = "SELECT * FROM orders WHERE cell = %s and done = %s"
        vars = (cell + 1, False)
        res = SqlQueryVarOne(conn,i,vars)
        
        if res == None:
          continue

        # refresh stacks for the rotators
        if 0 <= cell:
          stack_0.put(cell)
        if 1 <= cell:
          stack_1.put(cell)
        if 2 <= cell:
          stack_2.put(cell)

        # create thread to process the order
        if 0 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(stack_0, modbus_user, destino_flag_take_1, destino_flag_move_1, res[4]-1, res[5], res[6], res[7], res[8], res[9], res[10]))
          th.start()
        if 1 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(stack_1, modbus_user, destino_flag_take_2, destino_flag_move_2, res[4]-1, res[5], res[6], res[7], res[8], res[9], res[10]))
          th.start()
        if 2 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(stack_2, modbus_user, destino_flag_take_3, destino_flag_move_3, res[4]-1, res[5], res[6], res[7], res[8], res[9], res[10]))
          th.start()

        # update the database to signal order received
        i = "UPDATE orders SET done = %s WHERE id = %s"
        var = (True, res[0])
        res = SqlCreateVar(conn,i,var)

        machine_locks[machine_state] = True
        machine_locks[machine_state+1] = True 
        
        break

      # check if there is any free machine 1
      elif not machine_states[machine_state] and not machine_locks[machine_state]:
        # select a order for the machine 1 of the cell
        i = "SELECT * FROM orders WHERE cell = %s and maq_1 = %s and done = %s"
        vars = (cell + 1, True, False)
        res = SqlQueryVarOne(conn,i,vars)

        if res == None:
          continue

        # refresh stacks for the rotators
        if 0 <= cell:
          stack_0.put(cell)
        if 1 <= cell:
          stack_1.put(cell)
        if 2 <= cell:
          stack_2.put(cell)

        # create thread to process the order
        if 0 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(stack_0, modbus_user, destino_flag_take_1, destino_flag_move_1, res[4]-1, res[5], res[6], res[7], res[8], res[9], res[10]))
          th.start()
        if 1 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(stack_1, modbus_user, destino_flag_take_2, destino_flag_move_2, res[4]-1, res[5], res[6], res[7], res[8], res[9], res[10]))
          th.start()
        if 2 == cell:
          # create thread to process the order
          th = threading.Thread(target=handle_request, args=(stack_2, modbus_user, destino_flag_take_3, destino_flag_move_3, res[4]-1, res[5], res[6], res[7], res[8], res[9], res[10]))
          th.start()

        # update the database to signal order received
        i = "UPDATE orders SET done = %s WHERE id = %s"
        var = (True, res[0])
        res = SqlCreateVar(conn,i,var)

        machine_locks[machine_state] = True

        break

  SqlClose(conn)

# redirects the piece in the rotative belt into the cell
# requires: (rotative_id) id of the rotative belt to handle
# (destino_array) array of the pieces to pass by that belt
# returns: True if the information was passed to the belt
# False if the piece destination did not match the cell
def handle_destino(rotative_id, rotate):
  # check if the piece if for himself
  if rotate:
    write_modbus_register(Destino[rotative_id], 0)
    while read_modbus_coil(SRotate[rotative_id]):
      pass
    write_modbus_register(Destino[rotative_id], 2)
    return True
  else:
    write_modbus_register(Destino[rotative_id], 1)
    while read_modbus_coil(SRotate[rotative_id]):
      pass
    write_modbus_register(Destino[rotative_id], 2)
  return False

# destino manager
def destino_manager(modbus_user, destino_flag_take_1, destino_flag_take_2, destino_flag_take_3, destino_flag_move_1, destino_flag_move_2, destino_flag_move_3, stack_1, stack_2, stack_3):
  sensor_usage = [False for _ in range(len(SRotate))]
  while 1:
    modbus_user.wait()
    modbus_user.clear()
    rotate_sensors = []
    for i in range(len(SRotate)):
      rotate_sensors.append(read_modbus_coil(SRotate[i]))
    modbus_user.set()
    time.sleep(0.3)

    #print(stack_1.qsize(), stack_2.qsize(), stack_3.qsize(), rotate_sensors, sensor_usage, [destino_flag_take_1.isSet(), destino_flag_take_2.isSet(), destino_flag_take_3.isSet()], [destino_flag_move_1.isSet(), destino_flag_move_2.isSet(), destino_flag_move_3.isSet()])
    print(list(stack_1.queue), list(stack_2.queue), list(stack_3.queue))


    for rotate_sensor in range(len(rotate_sensors)):
      if not rotate_sensors[rotate_sensor]:
        sensor_usage[rotate_sensor] = False

    for rotate_sensor in range(len(rotate_sensors)):
      if rotate_sensors[rotate_sensor] and not sensor_usage[rotate_sensor]:
        sensor_usage[rotate_sensor] = True
        if rotate_sensor == 0:
          if stack_1.get() == 0:
            handle_destino(rotate_sensor, True)
            destino_flag_take_1.set()
          else:
            handle_destino(rotate_sensor, False)
        elif rotate_sensor == 1:
          if stack_2.get() == 1:
            handle_destino(rotate_sensor, True)
            destino_flag_take_2.set()
          else:
            handle_destino(rotate_sensor, False)
        elif rotate_sensor == 2:
          if stack_3.get() == 2:
            handle_destino(rotate_sensor, True)
            destino_flag_take_3.set()
          else:
            handle_destino(rotate_sensor, False)
      


# tests for handle_request
# threads
initial_config()

modbus_user = threading.Event()
modbus_user.set()

handle_scheduler(modbus_user)

'''th1 = threading.Thread(target=handle_request, args=(modbus_user, 0, True, 1, 7, True, 7, 8, [0]))
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
th4.join()'''


client_output.close()
client_coils.close()
