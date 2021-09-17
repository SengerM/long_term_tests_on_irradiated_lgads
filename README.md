# Long term stability studies on irradiated LGADs

This repository contains the software related with the *long term stability studies on irradiated LGADs* in use at UZH. 

## Installation

Just clone this repo wherever you like. You will need to install some extra "non-standard" packages, follow the installation instructions in each case:

- [Climate chamber control](https://github.com/SengerM/VotschTechnik-climate-chamber-Python).
- [CAENpy](https://github.com/SengerM/CAENpy).
- [Telegram progress reporter](https://github.com/SengerM/progressreporting). This will send important warnings to your Telegram account, so you can start the setup to run 24-7 and forget about it. If you don't want this, just remove all things related with Telegram in the file [daemon.py](daemon.py) and that's it.
- [grafica](https://github.com/SengerM/grafica).
- Other packages you can easily find on Google.

## Usage

The setup is composed by a "Votschtechnik (Weiss Technik) climate chamber", two CAEN desktop high voltage power supplies, a custom robotic system built for this application, and some additional hardware devices. To automatically control the setup use the [daemon.py](daemon.py) script. As the name suggests, it runs as a [daemon](https://en.wikipedia.org/wiki/Daemon_(computing)) in the background and controls/logs the setup. It is multi-threading and performs many things simultaneously.

### *Control* and *log* directories

To the daemon you must provide a *control directory* and a *log directory*. They can be located anywhere, you just have to provide the path to them. In the control directory you place a number of files with a specific structure and format that tell the configuration and how to operate. For examples see [this directory](doc/control_example). 

In the log directory you don't have to put anything, the daemon will create files there.

### Connection with the instruments

All the connections with the instruments and hardware devices is done within the `if __name__ == '__main__':` in the [daemon file](daemon.py). Currently most of the instruments are controlled through the network, if you want to change the connection method (e.g. to USB or whatever) you have to modify it here. For the CAEN power supplies the USB connection is already implemented (see [CAENpy](https://github.com/SengerM/CAENpy)).
