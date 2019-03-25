from ..core.base import config as __config, dc
from ..core.handler import ProcessorHandler

default_n_threads = int(__config.get('Preferences', 'number_of_thread'))


class Processor(ProcessorHandler):
    """The class has a role to interface between Scheduler and Manager
    If the process methods defined in this class is serially operated,
    all jobs will scheduled on scheduler and operate as single pipeline

    PyNIPT uses the home-designed data structure, we named is as 'Project folder',
    based on a BIDS standard <http://bids.neuroimgaing.io> for enabling to manage
    the set of the derivatives more efficiently.

    If you want more detail information, please refer our documentation in github.

    Args:
        bucket (:obj:'Bucket'):     Bucket instance.
        label (str):                The name of Pipeline package that this class will create.
        logger (bool):              True for logging the whole processes.


    Methods:
        get_daemon:                 get daemon for threading
        inspect_input:              check the integrity of input_path
        init_step:                  create a new step dir
        close_step:                 remove step dir if it empty
        clear:                      remove all empty step dir

    Attributes:
        scheduler_param:            scheduler config parameters
        summary:                    print out the summary of processor instance

    """
    def __init__(self, *args, **kwargs):
        if 'n_threads' in kwargs.keys():
            if kwargs['n_threads'] is None:
                self._n_threads = default_n_threads
            else:
                self._n_threads = kwargs.pop('n_threads')
        else:
            self._n_threads = default_n_threads
        super(Processor, self).__init__(*args, **kwargs)
        # install default interface in plugin folder

        # to control scheduling issues,
        self._waiting_list = []
        self._processed_list = []
        self._stepobjs = dict()

    @property
    def stepobjs(self):
        return self._stepobjs

    @property
    def scheduler_param(self):
        """The parameters will be taken by Scheduler class. """
        return dict(queue=self._waiting_list,
                    done=self._processed_list,
                    n_threads=self._n_threads)

    def get_daemon(self, func, *args, **kwargs):
        import threading
        daemon = threading.Thread(target=func, args=args, kwargs=kwargs)
        daemon.daemon = True
        daemon.start()
        return daemon

    def __repr__(self):
        return self.summary

    @property
    def summary(self):
        return str(self._summary())

    def _summary(self):
        """ Print summary information about the instance derived from this class.

        Returns:
            summary (str): summary information of the instance

        """
        s = list()
        s.append("** Summary of Processor instance initiated for [{}].\n".format(self.label))
        s.append("- Abspath of initiated package:\n\t{}".format(self.path))
        s.append("- The base dataclass of updated attributes:\n\t{}".format(dc[self._pre_idx]))
        s.append("- Available attributes:\n\t{}".format(self.bucket.param_keys[self._pre_idx]))
        if len(self._executed) is 0:
            pass
        else:
            s.append("- Processed steps:")
            for i, step in self._executed.items():
                s.append("\t{}: {}".format(i, step))
        if len(self._reported) is 0:
            pass
        else:
            s.append("- Reported steps:")
            for i, step in self._reported.items():
                s.append("\t{}: {}".format(i, step))
        if len(self._masked) is 0:
            pass
        else:
            s.append("- Masked data:")
            for i, step in self._masked.items():
                s.append("\t{}: {}".format(i, step))
        output = '\n'.join(s)
        return output


