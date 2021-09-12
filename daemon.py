from CAENpy.CAENDesktopHighVoltagePowerSupply import CAENDesktopHighVoltagePowerSupply, OneCAENChannel
from VotschTechnikClimateChamber.ClimateChamber import ClimateChamber
from pathlib import Path
import datetime
from time import sleep
import pandas
import threading
import numpy
import warnings
from importlib import reload
from data_processing_bureaucrat.Bureaucrat import TelegramReportingInformation
from progressreporting.TelegramProgressReporter import TelegramReporter

THREADS_SLEEP_TIME = 1
IV_CURVES_LOG_SUBDIR = 'IV_curves'

class LongTermSetupDaemon:
	def __init__(self, 
		caen_power_supplies: list,
		climate_chamber: ClimateChamber,
		log_data_directory: Path, 
		daemon_control_directory: Path, 
		voltage_ramp_speed_volts_per_second=5,
		standby_temperature_celsius = -20,
	):
		"""Parameters:
		- caen_power_supplies: list of CAENDesktopHighVoltagePowerSupply objects.
		- log_data_directory: Path to a directory in which all the log files will be stored. If the directory does not exist, it is created.
		- daemon_control_directory: Path to a directory in which to place files to configure the daemon and/or change its behavior."""
		for caen in caen_power_supplies:
			if not isinstance(caen, CAENDesktopHighVoltagePowerSupply):
				raise TypeError(f'At least one of the elements in <caen_power_supplies> is not an instance of {CAENDesktopHighVoltagePowerSupply}.')
		self._caen_power_supplies = {caen.serial_number: caen for caen in caen_power_supplies} # The keys of this dictionary are the serial numbers of each instrument.
		
		if not isinstance(climate_chamber, ClimateChamber):
			raise TypeError(f'<climate_chamber> must be an instance of {ClimateChamber}, received object of type {type(climate_chamber)} instead.')
		self.climate_chamber = climate_chamber
		
		self.standby_temperature_celsius = float(standby_temperature_celsius)
		
		self._this_daemon_timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
		
		# Directories ---
		self._log_dir_path = Path(log_data_directory)
		self._daemon_control_dir_path = Path(daemon_control_directory)
		for d in {self._log_dir_path, self._daemon_control_dir_path}:
			d.mkdir(parents = True, exist_ok = True)
		
		self._voltage_ramp_speed_volts_per_second = float(voltage_ramp_speed_volts_per_second)
		
		self._devices_configuration_df_lock = threading.RLock() # https://docs.python.org/3/library/threading.html#lock-objects
		
		self.load_devices_configuration_file() # Initialize loading the data.
		with self._devices_configuration_df_lock:
			self._device_lock = {dev: threading.RLock() for dev in self._devices_configuration_df.index} # This dictionary contains one "threading.Rlock()" object for each device that must be locked each time something is done (e.g. measuring an IV curve, a beta scan, etc.).
		
		self.telegram_reporter = TelegramReporter(
			telegram_token = TelegramReportingInformation().token, # Here I store the token of my bot hidden, never make it public.
			telegram_chat_id = '-529438836',
		)
		
	def run(self):
		self._keep_running = True
		
		def load_devices_configuration_file_thread_function():
			print(f'load_devices_configuration_file thread started.')
			while self._keep_running:
				status = self.load_devices_configuration_file()
				if status == 'configuration has changed':
					self.configure_channels()
				sleep(THREADS_SLEEP_TIME)
			print('load_devices_configuration_file finished.')
		
		def log_devices_standby_IV_thread_function():
			print(f'Starting log_all_devices_standby_IV thread...')
			while not hasattr(self, '_devices_configuration_df'):
				# Wait, it is still initializing.
				sleep(THREADS_SLEEP_TIME)
			print(f'log_devices_standby_IV thread started.')
			last_standby_log_of_device = {} # This dictionary will contain one key per device, the item will be a datetime.datetime.now() object registering the last logged data for such device.
			with self._devices_configuration_df_lock:
				for device_name in self._devices_configuration_df.index: # Initialize this by logging once each device.
					last_standby_log_of_device[device_name] = datetime.datetime.now()
			while self._keep_running:
				now = datetime.datetime.now()
				with self._devices_configuration_df_lock:
					for device_name in self._devices_configuration_df.index:
						if (now - last_standby_log_of_device[device_name]).seconds >= self._devices_configuration_df.loc[device_name, 'Log standby info every (s)']:
							# If this happens, we have to log this device.
							self.log_single_device_standby_IV(device_name)
							last_standby_log_of_device[device_name] = now
				sleep(THREADS_SLEEP_TIME)
			print('log_devices_standby_IV finished.')
		
		def measure_IV_curves_thread_function():
			print(f'Starting measure_IV_curves thread...')
			while not hasattr(self, '_devices_configuration_df'):
				# Wait, it is still initializing.
				sleep(THREADS_SLEEP_TIME)
			with self._devices_configuration_df_lock:
				devices = list(self._devices_configuration_df.index)
			last_IV_curve_for_device = {dev: datetime.datetime.now() for dev in devices} # Initialize.
			print(f'measure_IV_curves thread started.')
			while self._keep_running:
				with self._devices_configuration_df_lock:
					log_iv_curve_every = {dev: self._devices_configuration_df.loc[dev, 'IV_curve every (s)'] for dev in devices}
				for device in devices:
					if (datetime.datetime.now() - last_IV_curve_for_device[device]).total_seconds() >= log_iv_curve_every[device]:
						self.telegram_reporter.send_message(f'Measuring IV curve of device {device}.')
						self.log_single_device_standby_IV(device) # Log the info of this device before doing anything.
						try:
							self.measure_IV_curve(device_name = device)
							last_IV_curve_for_device[device] = datetime.datetime.now()
						except KeyboardInterrupt:
							raise KeyboardInterrupt()
						except Exception as e:
							error_message = f'Could not perform the measurement of the IV curve for device {device}, reason: "{e}".'
							print(error_message)
							self.telegram_reporter.send_message(error_message)
						finally:
							self.log_single_device_standby_IV(device) # Log the info of this device after the measurement.
				sleep(THREADS_SLEEP_TIME)
			print('measure_IV_curves finished.')
		
		def climate_chamber_temperature_thread_function():
			print(f'climate_chamber_temperature thread started.')
			while self._keep_running:
				sleep(60)
				try:
					temperature = self.climate_chamber.temperature_measured
				except KeyboardInterrupt:
					raise KeyboardInterrupt()
				except Exception as e:
					self.telegram_reporter.send_message(f'Cannot get temperature from climate chamber. Reason: {repr(e)}.')
					continue
				if temperature > -18:
					self.telegram_reporter.send_message(f'Temperature in the climate chamber is {temperature} °C.')
			print(f'climate_chamber_temperature thread finished.')
				
		
		self.telegram_reporter.send_message(f'Starting daemon...')
		threads = {
			'load_devices_configuration_file_thread': threading.Thread(target = load_devices_configuration_file_thread_function, daemon = True),
			'log_all_devices_standby_IV_thread': threading.Thread(target = log_devices_standby_IV_thread_function, daemon = True),
			'measure_IV_curves_thread': threading.Thread(target = measure_IV_curves_thread_function, daemon = True),
			'climate_chamber_temperature_thread': threading.Thread(target = climate_chamber_temperature_thread_function, daemon = True)
		}
		try:
			if self.climate_chamber.dryer == False or self.climate_chamber.compressed_air == False:
				raise RuntimeError('Please turn on the dryer and/or compressed air in the climate chamber before proceeding.')
			self.climate_chamber.temperature_set_point = self.standby_temperature_celsius
			self.climate_chamber.start()
			while ((self.climate_chamber.temperature_measured - self.standby_temperature_celsius)**2)**.5 > 2:
				# Wait for the chamber to be cold.
				self.telegram_reporter.send_message(f'Waiting for the chamber to cool down...\nTarget temperature: {self.standby_temperature_celsius:.1f} °C\nCurrent temperature: {self.climate_chamber.temperature_measured:.1f} °C')
				sleep(60)
			self.telegram_reporter.send_message(f'Temperature in the climate chamber is within +- 1 °C from standby temperature, proceeding with test.')
			self.configure_channels()
			for name,thread in threads.items():
				thread.name = name
				thread.start()
			self.telegram_reporter.send_message(f'Daemon started!')
			while all([t.is_alive() for t in list(threads.values())]): # If any thread dies due to some error, I want to stop everything.
				sleep(THREADS_SLEEP_TIME)
		except Exception as e:
			raise e
		finally:
			self.telegram_reporter.send_message(f'Exiting daemon...')
			self._keep_running = False
			while any([t.is_alive() for t in list(threads.values())]):
				print(f'Waiting for {sum([t.is_alive() for t in list(threads.values())])} threads to finish before exiting...')
				sleep(THREADS_SLEEP_TIME)
			self.telegram_reporter.send_message(f'The daemon is dead.')
		
	def configure_channels(self):
		""""Configures channel parameters such as the standby voltage and
		current compliance."""
		if not hasattr(self, '_devices_configuration_df'):
			raise RuntimeError(f'Before calling <configure_channels> you must load the configuration file usin <load_devices_configuration_file>.')
		with self._devices_configuration_df_lock:
			self._caen_outputs = {}
			for idx, row in self._devices_configuration_df.iterrows():
				caen = self._caen_power_supplies[row['CAEN serial number']]
				channel_number = row['Channel number']
				self._caen_outputs[idx] = OneCAENChannel(caen=caen, channel_number=channel_number)
			for idx, caen_output in self._caen_outputs.items():
				if self._device_lock[idx].acquire(blocking=False):
					caen_output.current_compliance = self._devices_configuration_df.loc[idx, 'Current compliance (A)']
					if caen_output.there_was_overcurrent: # To be able to use it again the voltage has to be brought to 0.
						caen_output.ramp_voltage(0)
					caen_output.output = 'on'
					for ramp_parameter in {'RUP','RDW'}:
						caen_output.set(PAR=ramp_parameter, VAL=self._voltage_ramp_speed_volts_per_second)
					sleep(THREADS_SLEEP_TIME)
					caen_output.V_set = self._devices_configuration_df.loc[idx, 'Standby voltage (V)']
					self._device_lock[idx].release()
	
	def load_devices_configuration_file(self):
		"""This method expects to find a file called "devices_configuration.xlsx"
		in the directory "self.daemon_control_dir_path" with a table with the
		following form (just an example): 
		
		"DEVICES CONFIGURATION FILE",1,2,3,4,5,6,7,8
		"Device name","#10","HPK PIN",,,,,,
		"CAEN model name","DT1470ET","DT1470ET",,,,,,
		"CAEN serial number",13398,13398,,,,,,
		"Channel number",0,1,,,,,,
		"Current compliance (A)",1.00E-06,1.00E-06,,,,,,
		"Standby voltage (V)",11,22,,,,,,
		"IV_curve start (V)",0,0,,,,,,
		"IV_curve stop (V)",11,22,,,,,,
		"IV_curve N_points",5,8,,,,,,
		"IV_curve every (s)",60,60,,,,,,
		
		"""
		with self._devices_configuration_df_lock:
			if hasattr(self, '_devices_configuration_df'):
				previous_config = self._devices_configuration_df.copy()
				is_this_the_first_call = False
			else:
				previous_config = pandas.DataFrame()
				is_this_the_first_call = True
			config_file_path = self._daemon_control_dir_path/Path('devices_configuration.xlsx')
			try:
				with warnings.catch_warnings(record=True):
					warnings.simplefilter("always")
					self._devices_configuration_df = pandas.read_excel(config_file_path,index_col='DEVICES CONFIGURATION FILE').T.reset_index(drop=True).dropna(how='all')
				self._devices_configuration_df = self._devices_configuration_df.astype(
					{
						'Device name': str,
						'CAEN model name': str,
						'CAEN serial number': str,
						'Channel number': int,
						'Current compliance (A)': float,
						'Standby voltage (V)': float,
						'IV_curve start (V)': float,
						'IV_curve stop (V)': float,
						'IV_curve N_points': int,
						'IV_curve every (s)': int,
						'Log standby info every (s)': float,
					}
				)
				self._devices_configuration_df.set_index('Device name', inplace=True)
			except FileNotFoundError:
				raise FileNotFoundError(f'Cannot find file <devices_configuration.xlsx> in path {config_file_path}. See the docstring of <load_devices_configuration_file> for more details.')
			if set(self._devices_configuration_df['CAEN serial number']) != set(self._caen_power_supplies):
				raise RuntimeError(f'The <devices_configuration.xlsx> file has CAEN serial numbers that are not being controlled by this daemon.')
			if len(set(self._devices_configuration_df.index)) != len(self._devices_configuration_df.index):
				raise RuntimeError(f'The <Device name> column of the <devices_configuration.xlsx> file has a repeated entry. This is clearly an error that must be fixed.')
			if not is_this_the_first_call and set(self._devices_configuration_df.index) != set(previous_config.index):
				raise RuntimeError(f'A device name was changed in the column <Device name> of the file <devices_configuration.xlsx>. This must be an error becasue you cannot change the devices without powering everything off.')
			status = 'no changes in the configuration'
			if not self._devices_configuration_df.equals(previous_config):
				# Store a copy of the new configuration with the timestamp.
				directory = self._log_dir_path/Path('configuration_history')
				directory.mkdir(parents=True, exist_ok=True)
				self._devices_configuration_df.to_csv(
					directory/Path(datetime.datetime.now().strftime('%Y%m%d%H%M%S')+'_devices_configuration.csv'), 
					index = False
				)
				status = 'configuration has changed'
		return status
	
	def log_single_device_standby_IV(self, device_name: str):
		if self._device_lock[device_name].acquire(blocking=False):
			with self._devices_configuration_df_lock:
				STANDBY_LOG_FNAME = 'standby_IV_log.csv'
				if device_name not in set(self._devices_configuration_df.index.values):
					raise ValueError(f'<device_name> received is "{device_name}" and is not present in the configured devices.')
				standby_log_fpath = self._log_dir_path/Path(STANDBY_LOG_FNAME)
				if not standby_log_fpath.is_file(): # Create the file.
					with open(self._log_dir_path/Path(STANDBY_LOG_FNAME), 'w') as ofile:
						print('When,Device name,Voltage (V),Current (A),Channel status byte', file = ofile)
				caen_output = self._caen_outputs[device_name]
				with open(self._log_dir_path/Path(STANDBY_LOG_FNAME), 'a') as ofile:
					print(f'{datetime.datetime.now()},{device_name},{caen_output.V_mon},{caen_output.I_mon},{caen_output.status_byte}', file=ofile)
			self._device_lock[device_name].release()
	
	def measure_IV_curve(self, device_name: str):
		"""Performs an IV curve measurement for the given device."""
		import measure_iv_with_CAEN # Import it locally because nobody else is going to use it.
		measure_iv_with_CAEN = reload(measure_iv_with_CAEN) # This is so I can modify the script without having to restart the daemon.
		with self._devices_configuration_df_lock:
			voltages_to_measure = numpy.linspace(
				self._devices_configuration_df.loc[device_name, 'IV_curve start (V)'],
				self._devices_configuration_df.loc[device_name, 'IV_curve stop (V)'],
				self._devices_configuration_df.loc[device_name, 'IV_curve N_points'],
			)
			compliance = float(self._devices_configuration_df.loc[device_name, 'Current compliance (A)'])
			standby_voltage = float(self._devices_configuration_df.loc[device_name, 'Standby voltage (V)'])
		if self._device_lock[device_name].acquire(blocking=False):
			caen_output = self._caen_outputs[device_name]
			measure_iv_with_CAEN.script_core(
				directory = self._log_dir_path/Path(IV_CURVES_LOG_SUBDIR)/Path(device_name),
				caen_channel = caen_output,
				voltages = voltages_to_measure, 
				current_compliance_amperes = compliance,
				caen_ramp_speed = self._voltage_ramp_speed_volts_per_second,
			)
			caen_output.ramp_voltage(
				voltage = standby_voltage,
				ramp_speed_VperSec = self._voltage_ramp_speed_volts_per_second,
			) # Go back to standby voltage.
			self._device_lock[device_name].release()
	
if __name__ == '__main__':
	
	print(f'Opening connections with the instruments...')
	caen_new = CAENDesktopHighVoltagePowerSupply(ip='130.60.165.119', timeout=10)
	caen_old = CAENDesktopHighVoltagePowerSupply(ip='130.60.165.121', timeout=10)
	print(f'Connections opened. Connected to:')
	for caen in [caen_new, caen_old]:
		print(f'\tCAEN {caen.model_name}, serial number {caen.serial_number}')
	
	daemon = LongTermSetupDaemon(
		caen_power_supplies = [
			caen_new,
			caen_old,
		],
		climate_chamber = ClimateChamber(
			ip = '130.60.165.218',
			temperature_min = -20,
			temperature_max = 20,
		),
		log_data_directory = '/home/sengerm/cernbox/measurements_data/LGAD/EPR2021_LGAD_long_term_test/daemon/log',
		daemon_control_directory = '/home/sengerm/cernbox/measurements_data/LGAD/EPR2021_LGAD_long_term_test/daemon/control',
	)
	
	print(f'Running daemon...')
	daemon.run()
