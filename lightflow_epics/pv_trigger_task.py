import time
from epics import PV
from collections import deque
from functools import partial

from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters


logger = get_logger(__name__)


class PvTriggerAction:
    """ Encapsulates the return value for the callback function of the PvTriggerTask.

    Hosts the data
    """
    def __init__(self, data, dags=None):
        """ Initialise the PvTriggerAction object.

        Args:
            data (MultiTaskData): Modified data that should be passed onto the
                                  triggered dags.
            dags (list): A list of dag names that should be started.
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
                 skip_initial_callback=True, *,
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
        super().__init__(name, force_run=force_run, propagate_skip=propagate_skip)
        self.params = TaskParameters(
            pv_name=pv_name,
            event_trigger_time=event_trigger_time,
            stop_polling_rate=stop_polling_rate,
            skip_initial_callback=skip_initial_callback
        )
        self._callable = callable

    def run(self, data, store, signal, **kwargs):
        params = self.params.eval(data, store)

        skipped_initial = False if params.skip_initial_callback else True
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
                event = queue.pop()
                if skipped_initial:
                    action = self._callable(data.copy(), store, **event)
                    if action is not None:
                        for dag in action.dags:
                            signal.start_dag(dag, data=action.data)
                else:
                    skipped_initial = True

        pv.clear_callbacks()

    @staticmethod
    def _pv_callback(queue, **kwargs):
        queue.append(kwargs)
