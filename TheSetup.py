from threading import RLock
from VotschTechnikClimateChamber.ClimateChamber import ClimateChamber # https://github.com/SengerM/VotschTechnik-climate-chamber-Python
from CAENpy.CAENDesktopHighVoltagePowerSupply import CAENDesktopHighVoltagePowerSupply, OneCAENChannel # https://github.com/SengerM/CAENpy
from SensirionSensor import SensirionSensor
import time

def check_integrity_of_slots_df(slots_df):
	"""Checks that there are no errors (like duplicate outputs) in the configuration dataframe for the slots. If there are no errors, this function does nothing. If an error is found, an error is raised. The checks that this function does are very specific of my setup."""
	if set(slots_df.columns) != {'Slot name', 'CAEN model name', 'CAEN serial number', 'CAEN channel number'}:
		raise ValueError(f'Wrong columns.')
	if set(slots_df['CAEN model name']) != {'DT1419ET', 'DT1470ET'}:
		raise ValueError(f'Wrong CAEN model name.')
	if set(slots_df['CAEN serial number']) != {'139', '13398'}:
		raise ValueError(f'Wrong CAEN serial number.')
	if set(slots_df['CAEN channel number']) != {0,1,2,3}:
		raise ValueError(f'Wrong CAEN channel number.')
	if len(set(slots_df['CAEN model name'].astype(str) + ',' + slots_df['CAEN serial number'].astype(str) + ',' + slots_df['CAEN channel number'].astype(str))) != 8:
		raise ValueError(f'There are errors in the slots_df, check for duplicate channel numbers or wrong CAEN model name or serial number.')

def _validate_type(var, var_name, typ):
	if not isinstance(var, typ):
		raise TypeError(f'<{var_name}> must be of type {typ}, received object of type {type(var)}.')

def _cast_to_float_number(var, var_name):
	err_msg = f'<{var_name}> must be of type {float}, received object of type {type(var)}.'
	try:
		float(var)
	except:
		raise TypeError(err_msg)
	try:
		len(var) # This is in case var is e.g. a numpy array.
	except:
		pass
	else:
		raise TypeError(err_msg)
	return float(var)

class TheSetup:
	"""The purpose of this class is to abstract the whole setup and provide easy and safe methods to control/read each variable. All technicalities regarding the handling of the individual outputs of each CAEN, climate chamber, etc. are supposed to be implemented here. The user of this class should worry about:
	- Controlling/monitoring the temperature in the climate chambers.
	- Controlling/monitoring the bias voltage/current in each of the 8 slots.
	- Move the robotic system with the beta source and the reference from one position to another.
	
	This class is responsible of being thread safe handling the hardware resources."""
	
	MAX_OPERATING_TEMPERATURE = -18 # °C. If the temperature is above this value, it is not possible to set a high voltage.
	UNBIASED_VOLTAGE_THRESHOLD = 5 # V. Value of bias voltage below which to consider that this is unbiased.
	NORMAL_OPERATING_TEMPERATURE = -20 # °C.
	
	def __init__(self, climate_chamber, sensirion_sensor, caen_1, caen_2, slots_df):
		"""- slots_df: a Pandas dataframe with columns "Slot name,CAEN model name,CAEN serial number,CAEN channel number"."""
		if not isinstance(climate_chamber, ClimateChamber):
			raise TypeError(f'<climate_chamber> must be an instance of {ClimateChamber}.')
		self._climate_chamber = climate_chamber
		if not isinstance(sensirion_sensor, SensirionSensor):
			raise TypeError(f'<sensirion_sensor> must be an instance of {SensirionSensor}.')
		self._sensirion_sensor = sensirion_sensor
		for caen in [caen_1, caen_2]:
			if not isinstance(caen, CAENDesktopHighVoltagePowerSupply):
				raise TypeError(f'<caen_1> and <caen_2> must be instances of {CAENDesktopHighVoltagePowerSupply}.')
		caen_power_supplies = {caen.serial_number: caen for caen in [caen_1, caen_2]} # The keys of this dictionary are the serial numbers of each instrument.
		
		slots_df = slots_df.copy() # Make a copy so to don't touch the original.
		slots_df.dropna()
		slots_df['Slot name'] = slots_df['Slot name'].astype(str)
		slots_df['CAEN model name'] = slots_df['CAEN model name'].astype(str)
		slots_df['CAEN serial number'] = slots_df['CAEN serial number'].astype(str)
		slots_df['CAEN channel number'] = slots_df['CAEN channel number'].astype(int)
		check_integrity_of_slots_df(slots_df)
		slots_df.set_index('Slot name', inplace=True)
		slots_df = slots_df.loc[slots_df.index != 'nan']
		self._caen_outputs_per_slot = {}
		for slot_name in slots_df.index:
			self._caen_outputs_per_slot[slot_name] = OneCAENChannel(
				caen = caen_power_supplies[str(slots_df.loc[slot_name, 'CAEN serial number'])],
				channel_number = int(slots_df.loc[slot_name, 'CAEN channel number']),
			)
		
	# Climatic related methods ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
	
	@property
	def temperature_set_point(self):
		"""Returns the temperature set point in Celsius."""
		return self._climate_chamber.temperature_set_point
	@temperature_set_point.setter
	def temperature_set_point(self, celsius):
		"""Set the temperature set point in Celsius. Before doing so it checks that all the slots are unbiased if the temperature is higher than the MAX_OPERATING_TEMPERATURE, otherwise it rises an error."""
		celsius = _cast_to_float_number(celsius, 'celsius')
		if celsius > self.MAX_OPERATING_TEMPERATURE and self._is_any_slot_biased():
			raise RuntimeError(f'Trying to se the temperature to {celsius} °C (which is above the maximum operating temperature of {self.MAX_OPERATING_TEMPERATURE} °C) while there are devices biased with a voltage greater than the "unbiased voltage threshold" of {self.UNBIASED_VOLTAGE_THRESHOLD} V.')
		self._climate_chamber.temperature_set_point = celsius
	
	@property
	def temperature(self):
		"""Returns the actual value of the temperature in Celsius."""
		return self._sensirion_sensor.temperature
	
	@property
	def humidity(self):
		"""Returns the actual value of the relative humidity in %RH units."""
		return self._sensirion_sensor.humidity
	
	@property
	def dryer(self):
		"""Returns the status of the dryer in the climate chamber, either True or False."""
		return self._climate_chamber.dryer
	
	@property
	def compressed_air(self):
		"""Returns the status of the compressed air in the climate chamber, either True or False."""
		return self._climate_chamber.compressed_air
	
	# Climatic related methods ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
	
	# High voltage methods ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
	
	def _get_CAEN_for_(self, slot_name: str):
		"""Returns an object of type `OneCAENChannel` (see https://github.com/SengerM/CAENpy) to control the corresponding slot name."""
		_validate_type(slot_name, 'slot_name', str)
		self._check_slot_name(slot_name)
		return self._caen_outputs_per_slot[slot_name]
	
	def set_bias_voltage(self, slot_name: str, volt: float):
		"""Set the bias voltage of the specified slot. Before doing so, it checks that the temperature is below the MAX_OPERATING_TEMPERATURE if the voltage is above what is considered as 0 V."""
		volt = _cast_to_float_number(volt, 'volt')
		if volt > self.UNBIASED_VOLTAGE_THRESHOLD:
			if self.status != 'ready to operate':
				raise RuntimeError(f'Trying to set bias voltage for slot {slot_name} to {volt} V while the setup status is {repr(self.status)}. It must be in status "ready to operate".')
			if self.temperature > self.MAX_OPERATING_TEMPERATURE:
				raise RuntimeError(f'Trying to set bias voltage for slot {slot_name} to {volt} V while the temperature is {self.temperature} °C, which is > than the maximum operating temperature of {self.MAX_OPERATING_TEMPERATURE} °C.')
		self._get_CAEN_for_(slot_name).V_set = float(volt)
	
	def set_current_compliance(self, slot_name: str, ampere: float):
		"""Set the current compliance for the specified slot."""
		self._get_CAEN_for_(slot_name).current_compliance = float(ampere)
	
	def measure_bias_voltage(self, slot_name: str):
		"""Returns a measurement of the bias voltage in the specified slot."""
		return self._get_CAEN_for_(slot_name).V_mon
	
	def measure_bias_current(self, slot_name: str):
		"""Returns a measurement of the bias current in the specified slot."""
		return self._get_CAEN_for_(slot_name).I_mon
	
	def _is_any_slot_biased(self):
		"""Returns True if any slot is biased (i.e. voltage higher than UNBIASED_VOLTAGE_THRESHOLD), otherwise returns False."""
		return any([(self.measure_bias_voltage(slot_name)**2)**.5 > self.UNBIASED_VOLTAGE_THRESHOLD for slot_name in self.slots_names])
	
	# High voltage methods ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
	
	# Robotic source+reference system methods ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
	
	def move_beta_source_to_position(self, x, y):
		"""Moves the beta source to the given position."""
		raise NotImplementedError('Not yet implemented.')
	
	# Robotic source+reference system methods ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
	
	# Setup state methods ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
	
	@property
	def status(self):
		"""Returns the status of the system based uniquely on the value of different variables, e.g. temperature. See the source code for more info."""
		ERROR_STATUS = 'error'
		if self._climate_chamber.is_running == False:
			return 'not running'
		else:
			if self.temperature_set_point < self.MAX_OPERATING_TEMPERATURE:
				if self.temperature < self.MAX_OPERATING_TEMPERATURE:
					return 'ready to operate'
				else: # if temperature > operating temperature
					if not self._is_any_slot_biased(): # if all unbiased
						return 'cooling down'
					else:
						return ERROR_STATUS
			else: # if temperature set point > operating temperature
				if self._is_any_slot_biased():
					return ERROR_STATUS
				else: # if all unbiased
					return 'warm'
	
	def start(self, humidity_before_cooling_down_percentage=20, humidity_timeout_seconds=60*10, cooling_down_timeout_seconds=60*15):
		"""Performs the routine to start the setup from "not operating, at room temperature" to "ready to operate at low temperature"."""
		_validate_type(humidity_before_cooling_down_percentage, 'humidity_before_cooling_down_percentage', int)
		_validate_type(humidity_timeout_seconds, 'humidity_timeout_seconds', int)
		_validate_type(cooling_down_timeout_seconds, 'cooling_down_timeout_seconds', int)
		if self.status != 'not running':
			raise RuntimeError(f'Before calling the `start` method the setup must be in "not running" status, currently it is in {repr(self.status)} status.')
		# If we are here it is because everything is fine, this means that all devices are unbiased and the climate chamber is at high temperature.
		self._climate_chamber.dryer = True
		self._climate_chamber.compressed_air = True
		self.temperature_set_point = 20 # We start at room temperature.
		for slot_name in self.slots_names:
			self.set_bias_voltage(slot_name, 0) # Just in case we set all bias voltages to 0 so we start fresh.
		self._climate_chamber.start() # Start the climate chamber. It is at room temperature but dryer is on so it should decrease the humidity.
		# Wait until humidity decreases ---
		time_started_drying = time.time()
		while self.humidity > humidity_before_cooling_down_percentage:
			time.sleep(11)
			if time.time()-time_started_drying > humidity_timeout_seconds:
				raise TimeoutError(f'Cannot reach target humidity of {humidity_before_cooling_down_percentage} %RH after {humidity_timeout_seconds} seconds of operation at room temperature, to proceed with the cooling down. Current humidity is {self.humidity:.2f} %RH.')
		# If we are here it is because the humidity is low enough to start cooling down ---
		self.temperature_set_point = self.NORMAL_OPERATING_TEMPERATURE
		time_started_cooling_down = time.time()
		time.sleep(1)
		if self.status != 'cooling down':
			raise RuntimeError(f'The status of the setup at this point should be "cooling down", instead it is {repr(self.status)}.')
		while self.status == 'cooling down':
			time.sleep(11)
			if time.time()-time_started_cooling_down > cooling_down_timeout_seconds:
				raise TimeoutError(f'Cannot reach target temperature of {self.NORMAL_OPERATING_TEMPERATURE} °C after {cooling_down_timeout_seconds} seconds. Current temperature is {self.temperature:.2f} °C.')
		# If we are here, everything should be ready to operate.
		time.sleep(1)
		if self.status != 'ready to operate':
			raise RuntimeError(f'The status of teh setup at this point should be "ready to operate", instead it is {repr(self.status)}.')
	
	def stop(self, unbias_devices_timeout_seconds=60*5, warm_up_timeout_seconds=60*15):
		_validate_type(unbias_devices_timeout_seconds, 'unbias_devices_timeout_seconds', int)
		_validate_type(warm_up_timeout_seconds, 'warm_up_timeout_seconds', int)
		# ~ if self.status != 'ready to operate':
			# ~ raise RuntimeError(f'Before calling the `stop` method the setup must be in "ready to operate" status, currently it is in {repr(self.status)} status.')
		# Set all the bias voltages to zero ---
		for slot_name in self.slots_names:
			self.set_bias_voltage(slot_name, 0)
		time_started_unbiasing = time.time()
		while self._is_any_slot_biased():
			time.sleep(11)
			if time.time()-time_started_unbiasing > unbias_devices_timeout_seconds:
				raise TimeoutError(f'I set the bias voltage of all the devices to 0 V and after {unbias_devices_timeout_seconds} seconds some of them are still biased...')
		# If we are here is because all the devices are already unbiased.
		self.temperature_set_point = 20 # Set room temperature.
		time_warm_up_started = time.time()
		while self.temperature < 18:
			time.sleep(11)
			if time.time()-time_warm_up_started > warm_up_timeout_seconds:
				raise TimeoutError(f'I started to warm up the setup {warm_up_timeout_seconds} seconds ago but it still does not reach room temperature. Current temperature is {self.temperature:.2f} °C.')
		# If we are here, the setup is already at room temperature.
		self._climate_chamber.stop()
		time.sleep(1)
		# If we are here, it should be safe to open the climate chamber.
		if self.status != 'not running':
			raise RuntimeError(f'The status of teh setup at this point should be "not running", instead it is {repr(self.status)}.')
	
	# Setup state methods ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
	
	@property
	def slots_names(self):
		return set(self._caen_outputs_per_slot.keys())
	
	def _check_slot_name(self, slot_name: str):
		"""if the slot_name is valid, this method does nothing, otherwise
		rises error."""
		_validate_type(slot_name, 'slot_name', str)
		if slot_name not in self.slots_names:
			raise ValueError(f'Wrong slot name {repr(slot_name)}. Valid slot names are {self.slots_names}.')

if __name__ == '__main__':
	import pandas
	import time
	
	slots_df = pandas.read_excel("/home/sengerm/cernbox/measurements_data/LGAD/EPR2021_LGAD_long_term_test/daemon/control/slots_definitions.ods", engine="odf")
	caen_new = CAENDesktopHighVoltagePowerSupply(ip='130.60.165.119', timeout=10)
	caen_old = CAENDesktopHighVoltagePowerSupply(ip='130.60.165.121', timeout=10)
	climate_chamber = ClimateChamber(ip = '130.60.165.218', temperature_min = -20, temperature_max = 20)
	sensirion_sensor = SensirionSensor()
	setup = TheSetup(
		climate_chamber = climate_chamber, 
		sensirion_sensor = sensirion_sensor, 
		caen_1 = caen_old, 
		caen_2 = caen_new, 
		slots_df = slots_df,
	)
	
	print(f'Setup status is "{setup.status}"')
	print(f'Climate chamber dryer: {setup.dryer}')
	print(f'Climate chamber compressed air: {setup.compressed_air}')
	print(f'Temperature set point: {setup.temperature_set_point} °C')
	print(f'Temperature: {setup.temperature:.2f} °C')
	print(f'Humidity: {setup.humidity:.2f} %RH')
	for idx, slot_name in enumerate(setup.slots_names):
		# ~ setup.set_bias_voltage(slot_name, 0)
		print(f'Bias voltage for slot {slot_name}: {setup.measure_bias_voltage(slot_name):.0f} V')
		print(f'Bias current for slot {slot_name}: {setup.measure_bias_current(slot_name)*1e-6:.2f} µA')
	print(f'Setup status is "{setup.status}"')
	
	print(f'Stopping the setup...')
	setup.stop()
	print(f'Setup is ready to open!')
	
	print(f'Setup status is "{setup.status}"')
	print(f'Climate chamber dryer: {setup.dryer}')
	print(f'Climate chamber compressed air: {setup.compressed_air}')
	print(f'Temperature set point: {setup.temperature_set_point} °C')
	print(f'Temperature: {setup.temperature:.2f} °C')
	print(f'Humidity: {setup.humidity:.2f} %RH')
	for idx, slot_name in enumerate(setup.slots_names):
		# ~ setup.set_bias_voltage(slot_name, 0)
		print(f'Bias voltage for slot {slot_name}: {setup.measure_bias_voltage(slot_name):.0f} V')
		print(f'Bias current for slot {slot_name}: {setup.measure_bias_current(slot_name)*1e-6:.2f} µA')
	print(f'Setup status is "{setup.status}"')
