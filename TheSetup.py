from threading import RLock
from VotschTechnikClimateChamber.ClimateChamber import ClimateChamber # https://github.com/SengerM/VotschTechnik-climate-chamber-Python
from CAENpy.CAENDesktopHighVoltagePowerSupply import CAENDesktopHighVoltagePowerSupply, OneCAENChannel # https://github.com/SengerM/CAENpy
from SensirionSensor import SensirionSensor

def check_integrity_of_slots_df(slots_df):
	"""Checks that there are no errors (like duplicate outputs) in the configuration dataframe for the slots. If there are no errors, this function does nothing. If an error is found, an error is raised. The checks that this function does are very specific of my setup."""
	if set(slots_df.columns) != {'Slot number', 'CAEN model name', 'CAEN serial number', 'CAEN channel number'}:
		raise ValueError(f'Wrong columns.')
	if sorted(list(slots_df['Slot number'])) != [1,2,3,4,5,6,7,8]:
		raise ValueError(f'Wrong slot numbers, they must be [1,2,3,4,5,6,7,8] but received {list(slots_df["Slot number"])}.')
	if set(slots_df['CAEN model name']) != {'DT1419ET', 'DT1470ET'}:
		raise ValueError(f'Wrong CAEN model name.')
	if set(slots_df['CAEN serial number']) != {139, 13398}:
		raise ValueError(f'Wrong CAEN serial number.')
	if set(slots_df['CAEN channel number']) != {0,1,2,3}:
		raise ValueError(f'Wrong CAEN channel number.')
	if len(set(slots_df['CAEN model name'].astype(str) + ',' + slots_df['CAEN serial number'].astype(str) + ',' + slots_df['CAEN channel number'].astype(str))) != 8:
		raise ValueError(f'There are errors in the slots_df, check for duplicate channel numbers or wrong CAEN model name or serial number.')

def _validate_type(var, var_name, typ):
	if not isinstance(var, typ):
		raise TypeError(f'<{var_name}> must be of type {typ}, received object of type {type(var)}.')

class TheSetup:
	"""The purpose of this class is to abstract the whole setup and provide easy methods to control/read each variable. All technicalities regarding the handling of the individual outputs of each CAEN, climate chamber, etc. are supposed to be implemented here. The user of this class should worry about:
	- Controlling/monitoring the temperature in the climate chambers.
	- Controlling/monitoring the bias voltage/current in each of the 8 slots.
	- Move the robotic system with the beta source and the reference from one position to another.
	
	This class is responsible of being thread safe handling the hardware resources."""
	
	def __init__(self, climate_chamber, sensirion_sensor, caen_1, caen_2, slots_df):
		"""- slots_df: a Pandas dataframe with columns "Slot number,CAEN model name,CAEN serial number,CAEN channel number"."""
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
		
		check_integrity_of_slots_df(slots_df)
		slots_df = slots_df.copy()
		slots_df.set_index('Slot number', inplace=True)
		self._caen_outputs_per_slot = {}
		for slot_number in slots_df.index:
			self._caen_outputs_per_slot[slot_number] = OneCAENChannel(
				caen = caen_power_supplies[str(slots_df.loc[slot_number, 'CAEN serial number'])],
				channel_number = slots_df.loc[slot_number, 'CAEN channel number'],
			)
	
	# Climatic related methods ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
	
	@property
	def temperature_set_point(self):
		"""Returns the temperature set point in Celsius."""
		return self._climate_chamber.temperature_set_point
	@temperature_set_point.setter
	def temperature_set_point(self, celsius):
		"""Set the temperature set point in Celsius."""
		self._climate_chamber.temperature_set_point = celsius
	
	@property
	def temperature(self):
		"""Returns the actual value of the temperature in Celsius."""
		return self._sensirion_sensor.temperature
	
	@property
	def humidity(self):
		"""Returns the actual value of the relative humidity in %RH units."""
		return self._sensirion_sensor.humidity
	
	# Climatic related methods ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
	
	# High voltage methods ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
	
	def set_bias_voltage(self, slot_number: int, volt: float):
		"""Set the bias voltage of the specified slot."""
		_validate_type(slot_number, 'slot_number', int)
		self._caen_outputs_per_slot[slot_number].V_set = float(volt)
	
	def set_current_compliance(self, slot_number: int, ampere: float):
		"""Set the current compliance for the specified slot."""
		_validate_type(slot_number, 'slot_number', int)
		self._caen_outputs_per_slot[slot_number].current_compliance = float(ampere)
	
	def measure_bias_voltage(self, slot_number: int):
		"""Returns a measurement of the bias voltage in the specified slot."""
		_validate_type(slot_number, 'slot_number', int)
		return self._caen_outputs_per_slot[slot_number].V_mon
	
	def measure_bias_current(self, slot_number: int):
		"""Returns a measurement of the bias current in the specified slot."""
		_validate_type(slot_number, 'slot_number', int)
		return self._caen_outputs_per_slot[slot_number].I_mon
	
	# High voltage methods ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
	
	# Robotic source+reference system methods ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
	
	def move_beta_source_to_position(self, x, y):
		"""Moves the beta source to the given position."""
		raise NotImplementedError('Not yet implemented.')
	
	# Robotic source+reference system methods ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑

if __name__ == '__main__':
	import pandas
	
	slots_df = pandas.read_excel("/home/sengerm/cernbox/measurements_data/LGAD/EPR2021_LGAD_long_term_test/daemon/control/slots_definition.ods", engine="odf")
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
	
	print(f'Temperature set point: {setup.temperature_set_point} °C')
	print(f'Temperature: {setup.temperature} °C')
	for slot_number in [1,2,3,4,5,6,7,8]:
		print(f'Voltage in slot {slot_number}: {setup.measure_bias_voltage(slot_number)} V')
		print(f'Bias current in slot {slot_number}: {setup.measure_bias_current(slot_number)} A')
	
	while True:
		setup.set_bias_voltage(int(input('Slot number to set bias voltag? ')), int(input('Bias voltage? ')))
		print('Changing bias voltage...')
