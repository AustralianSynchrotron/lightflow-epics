""" Watch an EPICS PV and start a dag if its value changes

This workflow demonstrates how to start a dag when a PV changes and its new value is
within a given range.

"""

import math

from lightflow.models import Parameters, Option, Dag
from lightflow.tasks import PythonTask
from lightflow_epics import PvTriggerTask

# the workflow requires the PV name to be supplied as an argument.
parameters = Parameters([
    Option('pvname', help='The PV to monitor', type=str)
])


# the callback function for the startup task.
def startup(data, store, signal, context):
    print('Starting the PV monitoring...')


# the callback function that is called when the PV changes. If the new value is
# in the range 3+-2, the name and value of the PV is stored in the data, the dag
# 'pv_action_dag' is started and the data is supplied to this dag.
def pv_callback(data, store, signal, context, pvname=None, value=None, **kwargs):
    print('Checking PV {} with value {}'.format(pvname, value))
    if math.fabs(value - 3.0) < 2.0:
        data['pv_name'] = pvname
        data['pv_value'] = value

        signal.start_dag('pv_action_dag', data=data)
        return data


# the callback function that prints the new PV value after it got changed.
def pv_printout(data, store, signal, context):
    print('PV {} has value: {}'.format(data['pv_name'], data['pv_value']))


# set up the PV monitoring dag.
pv_monitor_dag = Dag('pv_monitor_dag')

startup_task = PythonTask(name='startup_task',
                          callback=startup)

monitor_task = PvTriggerTask(name='monitor_task',
                             pv_name=lambda data, data_store: data_store.get('pvname'),
                             callback=pv_callback,
                             event_trigger_time=0.1,
                             stop_polling_rate=2,
                             skip_initial_callback=True)

pv_monitor_dag.define({startup_task: monitor_task})


# set up the PV action dag.
pv_action_dag = Dag('pv_action_dag', autostart=False)

printout_task = PythonTask(name='printout_task',
                           callback=pv_printout)

pv_action_dag.define({printout_task: None})
