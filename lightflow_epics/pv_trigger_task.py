import time
from epics import PV
from collections import deque
from functools import partial

from lightflow.queue import JobType
from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters, Action


logger = get_logger(__name__)


class PvTriggerTask(BaseTask):
    """ Triggers the execution of a callback function upon a change in a monitored PV.

    This trigger task monitors a PV for changes. If a change occurs a provided callback
    function is executed.
    """
    def __init__(self, name, pv_name, callback,
                 event_trigger_time=None, stop_polling_rate=2,
                 skip_initial_callback=True, *, queue=JobType.Task,
                 callback_init=None, callback_finally=None,
                 force_run=False, propagate_skip=True):
        """ Initialize the filesystem notify trigger task.

        All task parameters except the name, callback, queue, force_run and propagate_skip
        can either be their native type or a callable returning the native type.

        Args:
            name (str): The name of the task.
            pv_name (str, callable): The name of the PV that should be monitored.
            callback (callable): A callable object that is called when the PV changes.
                                 The function definition is
                                 def callback(data, store, signal, context, event)
                                 where event is the information returned by PyEPICS for
                                 a monitor callback event. 
            event_trigger_time (float, None): The waiting time between events in seconds.
                                              Set to None to turn off.
            stop_polling_rate (float): The number of events after which a signal is sent
                                       to the workflow to check whether the task
                                       should be stopped.
            skip_initial_callback (bool): Set to True to skip executing the callback
                                          upon initialization of the PV monitoring.
            queue (str): Name of the queue the task should be scheduled to. Defaults to
                         the general task queue.
            callback_init (callable): A callable that is called shortly before the task
                                      is run. The definition is:
                                        def (data, store, signal, context)
                                      where data the task data, store the workflow
                                      data store, signal the task signal and
                                      context the task context.
            callback_finally (callable): A callable that is always called at the end of
                                         a task, regardless whether it completed
                                         successfully, was stopped or was aborted.
                                         The definition is:
                                           def (status, data, store, signal, context)
                                         where status specifies whether the task was
                                           success: TaskStatus.Success
                                           stopped: TaskStatus.Stopped
                                           aborted: TaskStatus.Aborted
                                           raised exception: TaskStatus.Error
                                         data the task data, store the workflow
                                         data store, signal the task signal and
                                         context the task context.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, queue=queue,
                         callback_init=callback_init, callback_finally=callback_finally,
                         force_run=force_run, propagate_skip=propagate_skip)

        # set the tasks's parameters
        self.params = TaskParameters(
            pv_name=pv_name,
            event_trigger_time=event_trigger_time,
            stop_polling_rate=stop_polling_rate,
            skip_initial_callback=skip_initial_callback
        )
        self._callback = callback

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the PvTriggerTask task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            store (DataStoreDocument): The persistent data store object that allows the
                                       task to store data for access across the current
                                       workflow run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.
            context (TaskContext): The context in which the tasks runs.
        """
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
                if signal.is_stopped:
                    break

            # get all the events from the queue and call the callback function
            while len(queue) > 0:
                event = queue.pop()
                if skipped_initial:
                    if self._callback is not None:
                        self._callback(data, store, signal, context, **event)
                else:
                    skipped_initial = True

        pv.clear_callbacks()
        return Action(data)

    @staticmethod
    def _pv_callback(queue, **kwargs):
        """ Internal callback method for the PV monitoring. """
        queue.append(kwargs)
