import time
from epics import PV
from collections import deque
from functools import partial

from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters


logger = get_logger(__name__)


class PvTriggerAction:
    def __init__(self, data, dags=None):
        """ Initialise the Action object.

        Args:
            data (MultiTaskData): The processed data from the task that should be passed
                                  on to successor tasks.
            limit (list): A list of names of all immediate successor tasks that
                          should be executed.
        """
        self.data = data
        self.dags = dags if dags is not None else []


class PvTriggerTask(BaseTask):
    """ Triggers the execution of a DAG upon a change in a monitored PV.

    This trigger task monitors a PV for changes. If a change occurs a provded callable
    is executed
    """
    def __init__(self, name, pv_name, callable,
                 event_trigger_time=None, stop_polling_rate=2,
                 force_run=False, propagate_skip=True):
        """

        Args:
            name:
            pv_name:
            dag_callable:
            data_callable:
            force_run:
            propagate_skip:
        """
        super().__init__(name, force_run, propagate_skip)
        self.params = TaskParameters(
            pv_name=pv_name,
            event_trigger_time=event_trigger_time,
            stop_polling_rate=stop_polling_rate,
        )
        self._callable = callable

    def run(self, data, data_store, signal, **kwargs):
        params = self.params.eval(data, data_store)
        polling_event_number = 0
        queue = deque()

        # set up the internal callback
        pv = PV(params.pv_name, callback=partial(self._pv_callback, queue=queue))

        while True:
            if params.event_trigger_time is not None:
                time.sleep(params.event_trigger_time)

            # check every stop_polling_rate events the stop signal
            polling_event_number += 1
            if polling_event_number > params.stop_polling_rate:
                polling_event_number = 0
                if signal.is_stopped():
                    break

            # get all the events from the queue and call the callable
            while len(queue) > 0:
                action = self._callable(data.copy(), data_store, **queue.pop())
                if action is not None:
                    for dag in action.dags:
                        signal.run_dag(dag, data=action.data)

        pv.clear_callbacks()

    @staticmethod
    def _pv_callback(queue, **kwargs):
        queue.append(kwargs)
