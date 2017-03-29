import math

from lightflow.models import Arguments, Option, Dag
from lightflow.tasks import PythonTask
from lightflow_epics import PvTriggerTask, PvTriggerAction

arguments = Arguments([
    Option('pvname', help='The PV to monitor', type=str)
])


def startup(name, data, store, signal):
    print('Starting the PV monitoring...')


def pv_callback(data, store, pvname=None, value=None, **kwargs):
    print('Checking PV {} with value {}'.format(pvname, value))
    if math.fabs(value - 3.0) < 2.0:
        data['pv_name'] = pvname
        data['pv_value'] = value
        return PvTriggerAction(data, ['pv_action_dag'])


def pv_printout(name, data, store, signal):
    print('PV {} has value: {}'.format(data['pv_name'], data['pv_value']))


# PV monitoring DAG
pv_monitor_dag = Dag('pv_monitor_dag')

startup_task = PythonTask(name='startup_task',
                          callable=startup)

monitor_task = PvTriggerTask(name='monitor_task',
                             pv_name=lambda data, data_store: data_store.get('pvname'),
                             callable=pv_callback,
                             event_trigger_time=0.1, stop_polling_rate=2,
                             skip_initial_callback=True)

pv_monitor_dag.define({startup_task: monitor_task})


# PV action DAG
pv_action_dag = Dag('pv_action_dag', autostart=False)

printout_task = PythonTask(name='printout_task',
                           callable=pv_printout)

pv_action_dag.define({printout_task: None})
