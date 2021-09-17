import pandas
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import grafica
from pathlib import Path

TIME_FORMATS = {'%Y-%m-%d', '%Y-%m-%d-%H', '%Y-%m-%d-%H:%M'}

COLORS = [
	'#0b00a3',
	'#c92e2e',
	'#02b013',
	'#f005e4',
	'#e6c007',
	'#00bab4',
	'#000000',
	'#7a0060',
	'#44e632',
]

def script_core(From, To, max_points_per_device=None):
	data_df = pandas.read_csv('~/cernbox/measurements_data/LGAD/EPR2021_LGAD_long_term_test/daemon/log/standby_IV_log.csv')
	data_df['When'] = pandas.to_datetime(data_df['When'])
	data_df['Voltage (V)'] = (data_df['Voltage (V)']**2)**.5

	devices_df = pandas.read_excel(
		'~/cernbox/UZH devices/EPR 2021/EPR 2021 from Torino.xlsx',
		sheet_name = 'FBK',
	).set_index('#')

	

	plotly_figure = make_subplots(rows = 2, shared_xaxes = True, vertical_spacing = 0.02)
	grafica_figure = grafica.new()
	grafica_figure.plotly_figure = plotly_figure
	grafica_figure.title = 'Standby measurements'
	plotly_figure.update_xaxes(
		title_text = 'When',
		row = 2,
		col = 1,
	)
	plotly_figure.update_yaxes(
		title_text = 'Bias current (A)',
		row = 1,
		col = 1,
	)
	plotly_figure.update_yaxes(
		title_text = 'Bias voltage (V)',
		row = 2,
		col = 1,
	)
	for i,device in enumerate(sorted(set(data_df['Device name']))):
		try:
			device_number = int(device.replace('#',''))
		except:
			device_number = -1
		if device_number not in devices_df.index:
			continue
		label_for_plots = f'#{device_number}, W{devices_df.loc[device_number,"Wafer"]} T{devices_df.loc[device_number,"Type"]} {devices_df.loc[device_number,"Fluence/1e14"]}e14 n<sub>eq</sub> cm<sup>-2</sup>'
		data_to_plot_df = data_df.loc[(data_df['Device name']==device)&(data_df['When']>=From)&(data_df['When']<=To)]
		if max_points_per_device is not None:
			data_to_plot_df = data_to_plot_df.sample(n=max_points_per_device)
		data_to_plot_df.sort_values(by='When', inplace=True)
		for magnitude in ['Current (A)', 'Voltage (V)']:
			plotly_figure.add_trace(
				go.Scatter(
					x = data_to_plot_df['When'],
					y = data_to_plot_df[magnitude],
					mode = 'markers+lines',
					name = label_for_plots,
					showlegend = True if magnitude == 'Current (A)' else False,
					legendgroup = device,
				),
				row = 1 if magnitude == 'Current (A)' else 2,
				col = 1,
			)
			plotly_figure['data'][-1]['marker']['color'] = COLORS[i]
	grafica.save_unsaved(mkdir=Path.home()/Path('cernbox/measurements_data/LGAD/EPR2021_LGAD_long_term_test/daemon/log'))

########################################################################

if __name__ == '__main__':
	import argparse
	import datetime
	
	parser = argparse.ArgumentParser(
		description = 'Plots the standby data.'
	)
	parser.add_argument(
		'--from',
		metavar = 'when', 
		help = f'Plot data starting from this date. Default is the big bang. Available formats: {TIME_FORMATS}, see https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes.',
		required = False,
		default = '1999-01-01',
		dest = f'From',
		type = str,
	)
	parser.add_argument(
		'--to',
		metavar = 'when', 
		help = 'Plot data starting from this date. Default is the big bang. Available formats: {TIME_FORMATS}, see https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes.',
		required = False,
		default = '2222-01-01',
		dest = 'To',
		type = str,
	)
	parser.add_argument(
		'--max-points-per-device',
		metavar = 'N', 
		help = 'Maximum number of data points to plot for each device. When the dataset is too big, plotting 99999 points for each event can make the plots unresponsive. This argument limits the number of data points to plot. The data points are chosen randomly from the data set. Default is all data points.',
		required = False,
		default = None,
		dest = 'max_points',
		type = int,
	)

	args = parser.parse_args()
	
	args_from_to = {
		'From': args.From,
		'To': args.To,
	}
	dates_from_to = {}
	for fromto in args_from_to:
		if args_from_to[fromto] == 'today':
			dates_from_to[fromto] = datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'), "%Y-%m-%d")
		else:
			for fmat in TIME_FORMATS:
				try:
					dates_from_to[fromto] = datetime.datetime.strptime(args_from_to[fromto], fmat)
				except:
					pass

	From = dates_from_to['From']
	To = dates_from_to['To']
	
	script_core(From, To, args.max_points)
