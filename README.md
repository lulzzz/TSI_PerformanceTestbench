# TSI_PerformanceTestbench
Performance test bench for time series databases

## Getting Started
The tested database should be installed and configurated on Hyper-V virtual machine before testing. Also, client implementation needs to be done on test bench source code. The client can be implemented by creating a new class which inherits IAdapter interface. By creating the body for the IAdapter functions the client is integrated into the test bench. All database specific functions can be implemented in that class also.

**_HINT!_** Check TSI_PerformanceTesting_Guide pdf for more detailed information

### Prerequisites
- Windows machine with Hyper-V

### Installing
- Database under test needs to be installed into a VM
- (If not supported yet) Database client implementation needs to be done to the test bench

### Runnning the tests

The test configurations are modified through the configuration file which uses .ini format. Example ini file with all possible settings is provided in the repository.

Syntax: TSI_PerformanceTestbench _[ini file]_:

```
TSI_PerformanceTestbench TSItestbench.ini
```

### Results
The results are located in the current user's document folder under TSI folder
```
e.g. C:\Users\amdin\Documents\TSI\
```
Under TSI folder each test run has its own folder.

### Currenlty supported Databases
- ABB's cpmPlus History
- InfluxDB (v1.6)
- TimescaleDB (0.11)
