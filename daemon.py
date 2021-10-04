from pathlib import Path
import datetime
from time import sleep
import pandas
import threading
import numpy
import warnings
from importlib import reload
from data_processing_bureaucrat.Bureaucrat import TelegramReportingInformation # Here I store privately the token of my Telegram bot.
from progressreporting.TelegramProgressReporter import TelegramReporter # https://github.com/SengerM/progressreporting
from TheSetup import TheSetup
from plot_standby_logged_data import script_core as plot_standby_logged_data

THREADS_SLEEP_TIME = 1
IV_CURVES_LOG_SUBDIR = 'IV_curves'

def read_devices_configuration_file(path):
	with warnings.catch_warnings(record=True):
		warnings.simplefilter("always")
		df = pandas.read_excel(path, index_col='Slot number', engine='odf').T.reset_index(drop=True).dropna(how='all')
	df = df.astype(
		{
			'Slot name': str,
			'Current compliance (A)': float,
			'Standby voltage (V)': float,
			'IV_curve start (V)': float,
			'IV_curve stop (V)': float,
			'IV_curve N_points': int,
			'IV_curve every (s)': int,
			'Log standby info every (s)': float,
		}
	)
	df.set_index('Slot name', inplace=True)
	return df

class LongTermSetupDaemon:
	def __init__(self, the_setup: TheSetup, log_data_directory: Path, daemon_control_directory: Path, voltage_ramp_speed_volts_per_second = 5):
		if not isinstance(the_setup, TheSetup):
			raise TypeError(f'<the_setup> must be an instance of {TheSetup}, received object of type {type(the_setup)}.')
		self.the_setup = the_setup
		
		self._voltage_ramp_speed_volts_per_second = float(voltage_ramp_speed_volts_per_second)
		
		self._this_daemon_timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
		
		# Directories ---
		self._log_dir_path = Path(log_data_directory)
		self._daemon_control_dir_path = Path(daemon_control_directory)
		for d in {self._log_dir_path, self._daemon_control_dir_path}:
			d.mkdir(parents = True, exist_ok = True)
		
		self._slots_locks = {slot: threading.RLock() for slot in the_setup.slots_names} # This dictionary contains one "threading.Rlock" object for each slot that must be locked each time something is done (e.g. measuring an IV curve, a beta scan, etc.).
		
		self.telegram_reporter = TelegramReporter(
			telegram_token = TelegramReportingInformation().token, # Here I store the token of my bot hidden, never make it public.
			telegram_chat_id = '-529438836',
		)
		
	@property
	def climatic_df(self):
		"""This method returns a new copy of the climatic dataframe. The copy returned is up to date with the contents of the climatic configuration file. If there were recent changes in the file, it is automatically reloaded."""
		file_path = self._daemon_control_dir_path/Path("climatic.ods")
		if not hasattr(self, '__climatic_configuration_df_lock'): # We will enter here only the first time this method is called.
			self.__climatic_configuration_df_lock = threading.RLock() # This is to make this method thread-safe as it will probably be called from many threads.
			self.__last_time_the_climatic_file_was_updated = datetime.datetime(year=1999, month=1, day=1) # Just initializing this.
		with self.__climatic_configuration_df_lock:
			modification_datetime_of_the_file = datetime.datetime.fromtimestamp(file_path.stat().st_mtime)
			if modification_datetime_of_the_file > self.__last_time_the_climatic_file_was_updated: # If there is a newer version...
				self.__climatic_df = pandas.read_excel(file_path, engine="odf") # Update our dataframe.
				self.__last_time_the_climatic_file_was_updated = datetime.datetime.now() # Update the last-update-datetime.
			return self.__climatic_df.copy()
	
	@property
	def devices_configuration_df(self):
		"""This method returns a new copy of the configuration dataframe. The copy returned is up to date with the contents of the devices configuration file. If there were recent changes in the file, it is automatically reloaded."""
		file_path = self._daemon_control_dir_path/Path('devices_configuration.ods')
		if not hasattr(self, '__devices_configuration_df_lock'): # We will enter here only the first time this method is called.
			self.__devices_configuration_df_lock = threading.RLock() # This is to make this method thread-safe as it will probably be called from many threads.
			self.__last_time_the_configuration_file_was_updated = datetime.datetime(year=1999, month=1, day=1) # Just initializing this.
		with self.__devices_configuration_df_lock:
			modification_datetime_of_the_file = datetime.datetime.fromtimestamp(file_path.stat().st_mtime)
			if modification_datetime_of_the_file > self.__last_time_the_configuration_file_was_updated: # If there is a newer version...
				self.__devices_configuration_df = read_devices_configuration_file(file_path) # Update our dataframe.
				self.__last_time_the_configuration_file_was_updated = datetime.datetime.now() # Update the last-update-datetime.
			return self.__devices_configuration_df.copy()
	
	def run(self):
		self._keep_running = True
		
		def update_devices_standby_conditions_thread_function():
			"""This thread updates the configuration of each of the devices according to what is specified in the configuration file."""
			log_directory_path = self._log_dir_path/Path('configuration_history')
			log_directory_path.mkdir(parents=True, exist_ok=True) # Create it if it does not exist.
			previous_configuration_df = self.devices_configuration_df # Initializing this.
			first_iteration = True
			while self._keep_running:
				sleep(THREADS_SLEEP_TIME)
				if self.the_setup.status != 'ready to operate': continue # If the setup is not operating (e.g. it is warmed up) we don't have to do anything.
				# If we are here it is because the setup is "ready to operate". Now we have to act.
				current_configuration_df = self.devices_configuration_df
				there_was_a_change_in_the_configuration = not current_configuration_df.equals(previous_configuration_df)
				if there_was_a_change_in_the_configuration or first_iteration:
					current_configuration_df.to_csv(log_directory_path/Path(datetime.datetime.now().strftime('%Y%m%d%H%M%S')+'_devices_configuration.csv'))
					for slot_name in self.the_setup.slots_names:
						self.configure_single_device_standby(slot_name)
				previous_configuration_df = current_configuration_df # Update the previous configuration for the next iteration.
				first_iteration = False
		
		def update_chamber_temperature_thread_function():
			previous_configuration_df = self.climatic_df # Initializing this.
			first_iteration = True
			while self._keep_running:
				sleep(THREADS_SLEEP_TIME)
				if self.the_setup.status != 'ready to operate': continue # If the setup is not operating (e.g. it is warmed up) we don't have to do anything.
				# If we are here it is because the setup is "ready to operate". Now we have to act.
				current_configuration_df = self.climatic_df
				there_was_a_change_in_the_configuration = not current_configuration_df.equals(previous_configuration_df)
				if there_was_a_change_in_the_configuration or first_iteration:
					self.the_setup.temperature_set_point = current_configuration_df.loc[0,'Standby temperature (°C)']
				previous_configuration_df = current_configuration_df # Update the previous configuration for the next iteration.
				first_iteration = False
		
		def log_devices_standby_IV_thread_function():
			last_standby_log_of_device = {} # This dictionary will contain one key per device, the item will be a datetime.datetime.now() object registering the last logged data for such device.
			for slot_name in self.the_setup.slots_names:
				self.log_single_device_standby_IV(slot_name) # Initialize this by logging once each device.
				last_standby_log_of_device[slot_name] = datetime.datetime.now()
			while self._keep_running:
				sleep(THREADS_SLEEP_TIME)
				for slot_name in self.the_setup.slots_names:
					if (datetime.datetime.now() - last_standby_log_of_device[slot_name]).seconds >= self.devices_configuration_df.loc[slot_name, 'Log standby info every (s)']: # If this happens, we have to log this device.
						self.log_single_device_standby_IV(slot_name)
						last_standby_log_of_device[slot_name] = datetime.datetime.now()
		
		def log_climatic_data_thread_function():
			self.log_climatic_data()
			last_climatic_log = datetime.datetime.now()
			while self._keep_running:
				sleep(THREADS_SLEEP_TIME)
				if (datetime.datetime.now() - last_climatic_log).seconds >= self.climatic_df.loc[0,'Log data every (s)']:
					self.log_climatic_data()
					last_climatic_log = datetime.datetime.now()
		
		def start_stop_test_thread_function():
			"""This thread starts-stops the setup depending on the configuration file in the control directory. If there is a file named `setup.run` the setup starts, if the file is called `setup.stop` the setup stops, otherwise a warning is displayed."""
			def check_what_to_do_config():
				if (self._daemon_control_dir_path/Path('setup.run')).is_file():
					return 'run test'
				elif (self._daemon_control_dir_path/Path('setup.stop')).is_file():
					return 'stop test'
				else:
					return 'undefined'
			while self._keep_running:
				sleep(THREADS_SLEEP_TIME)
				what_to_do = check_what_to_do_config()
				if what_to_do == 'run test' and self.the_setup.status == 'not running':
					self.the_setup.start()
				elif what_to_do == 'stop test' and self.the_setup.status != 'not running':
					self.the_setup.stop()
					msg = f'Setup has been stopped, it is safe to open. ✅'
					self.telegram_reporter.send_message(msg)
					print(msg)
				elif what_to_do == 'undefined':
					self.telegram_reporter.send_message(f'I dont konw what to do, please check the file {self._daemon_control_dir_path/Path("setup.WHAT_TO_DO")}.')
		
		def temperature_monitoring_thread_function():
			"""This thread constantly monitors the temperature of the setup and the status. In case of inconsistencies it will start spamming with warnings."""
			TEMPERATURE_THRESHOLD = -18
			BIAS_VOLTAGE_THRESHOLD = 5
			CHECKING_TIME_IN_SECONDS = 60
			last_check = datetime.datetime.now()
			while self._keep_running:
				if (datetime.datetime.now()-last_check).seconds > CHECKING_TIME_IN_SECONDS:
					last_check = datetime.datetime.now()
					any_device_is_biased = any([self.the_setup.measure_bias_voltage(slot_name)>BIAS_VOLTAGE_THRESHOLD for slot_name in self.the_setup.slots_names])
					temperature_is_higher_than_temperature_threshold = self.the_setup.temperature > TEMPERATURE_THRESHOLD
					if any_device_is_biased and temperature_is_higher_than_temperature_threshold:
						msg = f'⚠️ WARNING: Temperature is {self.the_setup.temperature} °C and there are devices still biased with more than {BIAS_VOLTAGE_THRESHOLD} V.'
						self.telegram_reporter.send_message(msg)
						warnings.warn(msg)
				sleep(THREADS_SLEEP_TIME)
		
		def plot_standby_data_thread_function():
			"""This thread just makes the plot periodically."""
			last_24hour_plot_made_datetime = datetime.datetime(year=1999, month=1, day=1) # Just initializing this.
			last_weekly_plot_made_datetime = last_24hour_plot_made_datetime # Just initializing...
			while self._keep_running:
				sleep(THREADS_SLEEP_TIME)
				try:
					with warnings.catch_warnings():
						warnings.simplefilter("ignore")
						if (datetime.datetime.now() - last_24hour_plot_made_datetime).seconds > 33:
							plot_standby_logged_data(
								From = datetime.datetime.now() - datetime.timedelta(days = 1),
								To = datetime.datetime.now(),
								ofname = str(self._log_dir_path/Path('last_24_hours'))
							)
							last_24hour_plot_made_datetime = datetime.datetime.now()
						
						if (datetime.datetime.now() - last_weekly_plot_made_datetime).days > 0:
							plot_standby_logged_data(
								From = datetime.datetime.now() - datetime.timedelta(days = 7),
								To = datetime.datetime.now(),
								ofname = str(self._log_dir_path/Path('last_7_days'))
							)
							last_weekly_plot_made_datetime = datetime.datetime.now()
				except Exception as e:
					warnings.warn(f'Cannot plot logged data, reason: {e}.')
				
		threads = {
			'update_devices_standby_conditions_thread': threading.Thread(target = update_devices_standby_conditions_thread_function, daemon = True),
			'update_chamber_temperature_thread': threading.Thread(target = update_chamber_temperature_thread_function, daemon = True),
			'log_devices_standby_IV_thread': threading.Thread(target = log_devices_standby_IV_thread_function, daemon = True),
			'log_climatic_data_thread': threading.Thread(target = log_climatic_data_thread_function, daemon = True),
			'start_stop_test_thread': threading.Thread(target = start_stop_test_thread_function, daemon = True),
			'temperature_monitoring_thread': threading.Thread(target = temperature_monitoring_thread_function, daemon = True),
			'plot_standby_data_thread': threading.Thread(target = plot_standby_data_thread_function, daemon = True),
		}
		try: # All the execution must be inside this try statement, so the Telegram reporter will report in case of failure.
			for name,thread in threads.items():
				thread.name = name
				thread.start()
			msg = f'Daemon has started! 😈 🤘'
			print(msg)
			self.telegram_reporter.send_message(msg)
			while all([t.is_alive() for t in list(threads.values())]): # If any thread dies due to some error, I want to stop everything.
				sleep(THREADS_SLEEP_TIME)
		except Exception as e:
			raise e
		finally:
			self._keep_running = False # Tell all other threads to stop.
			self.telegram_reporter.send_message(f'Exiting daemon, waiting for all threads to finish...') # Report that something has happened.
			while any([t.is_alive() for t in list(threads.values())]): # Wait until all threads have finished.
				print(f'Waiting for {sum([t.is_alive() for t in list(threads.values())])} threads to finish before exiting...')
				sleep(THREADS_SLEEP_TIME)
			self.telegram_reporter.send_message(f'The daemon is dead. 👹 ⚰ 👿')
	
	def configure_single_device_standby(self, slot_name: str):
		"""Given a slot_name, this method configures such slot according
		to what is specified in the "devices_configuration_df". I.e. it
		sets the compliance current, the bias voltage, etc."""
		if self._slots_locks[slot_name].acquire(blocking=False):
			devices_configuration_df = self.devices_configuration_df # Get a new copy to freeze it in case it is modified in the meantime.
			self.the_setup.set_current_compliance(slot_name, devices_configuration_df.loc[slot_name, 'Current compliance (A)'])
			self.the_setup.slot_output(slot_name, 'on') # Turn on the output.
			self.the_setup.set_ramp_speed(slot_name, self._voltage_ramp_speed_volts_per_second)
			self.the_setup.set_bias_voltage(slot_name, devices_configuration_df.loc[slot_name, 'Standby voltage (V)'])
			self._slots_locks[slot_name].release()
	
	def log_single_device_standby_IV(self, slot_name: str):
		standby_log_fpath = self._log_dir_path/Path('standby_IV_log.csv')
		if self._slots_locks[slot_name].acquire(blocking=False):
			if not standby_log_fpath.is_file(): # Create the file if it does not exist.
				with open(standby_log_fpath, 'w') as ofile:
					print('When,Device name,Voltage (V),Current (A),Channel status byte', file = ofile)
			with open(standby_log_fpath, 'a') as ofile:
				print(f'{datetime.datetime.now()},{slot_name},{self.the_setup.measure_bias_voltage(slot_name)},{self.the_setup.measure_bias_current(slot_name)},{self.the_setup.CAEN_status_byte(slot_name)}', file=ofile)
			self._slots_locks[slot_name].release()
	
	def log_climatic_data(self):
		climatic_log_fpath = self._log_dir_path/Path('climatic_log.csv')
		if not climatic_log_fpath.is_file(): # Create the file.
			with open(climatic_log_fpath, 'w') as ofile:
				print('When,Temperature (°C),Humidity (%RH), Temperature set point (°C)', file = ofile)
		with open(climatic_log_fpath, 'a') as ofile:
			print(f'{datetime.datetime.now()},{self.the_setup.temperature},{self.the_setup.humidity},{self.the_setup.temperature_set_point}', file = ofile)
		
if __name__ == '__main__':
	from VotschTechnikClimateChamber.ClimateChamber import ClimateChamber # https://github.com/SengerM/VotschTechnik-climate-chamber-Python
	from CAENpy.CAENDesktopHighVoltagePowerSupply import CAENDesktopHighVoltagePowerSupply, OneCAENChannel # https://github.com/SengerM/CAENpy
	from SensirionSensor import SensirionSensor
	
	caen_new = CAENDesktopHighVoltagePowerSupply(ip='130.60.165.119', timeout=10)
	caen_old = CAENDesktopHighVoltagePowerSupply(ip='130.60.165.121', timeout=10)
	climate_chamber = ClimateChamber(ip = '130.60.165.218', temperature_min = -20, temperature_max = 20)
	sensirion_sensor = SensirionSensor()
	setup = TheSetup(
		climate_chamber = climate_chamber, 
		sensirion_sensor = sensirion_sensor, 
		caen_1 = caen_old, 
		caen_2 = caen_new, 
		slots_df = pandas.read_excel('/home/sengerm/cernbox/projects/LGAD_stability/daemon/control/slots_definitions.ods', engine="odf"),
	)
	
	daemon = LongTermSetupDaemon(
		the_setup = setup,
		log_data_directory = '/home/sengerm/cernbox/projects/LGAD_stability/daemon/log',
		daemon_control_directory = '/home/sengerm/cernbox/projects/LGAD_stability/daemon/control',
	)
	
	print(f'Running daemon...')
	daemon.run()
