# Tai Sakuma <sakuma@cern.ch>
import sys
import logging
import collections

import ROOT

import alphatwirl

ROOT.gROOT.SetBatch(1)

##__________________________________________________________________||
import logging
logger = logging.getLogger(__name__)
log_handler = logging.StreamHandler(stream=sys.stdout)
log_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

##__________________________________________________________________||
from parallel import build_parallel
from profile_func import profile_func

##__________________________________________________________________||
class FrameworkCMSEDM(object):
    """A simple framework for using alphatwirl

    """
    def __init__(self,
                 quiet = False,
                 parallel_mode = 'multiprocessing',
                 htcondor_job_desc_extra = [ ],
                 process = 8,
                 user_modules = (),
                 max_events_per_dataset = -1,
                 max_events_per_process = -1,
                 max_files_per_dataset = -1,
                 max_files_per_process = 1,
                 profile = False,
                 profile_out_path = None
    ):
        user_modules = set(user_modules)
        user_modules.add('framework_cmsedm')
        user_modules.add('profile_func')
        self.parallel = build_parallel(
            parallel_mode = parallel_mode,
            quiet = quiet,
            processes = process,
            user_modules = user_modules,
            htcondor_job_desc_extra = htcondor_job_desc_extra,
        )
        self.max_events_per_dataset = max_events_per_dataset
        self.max_events_per_process = max_events_per_process
        self.max_files_per_dataset = max_files_per_dataset
        self.max_files_per_process = max_files_per_process
        self.profile = profile
        self.profile_out_path = profile_out_path

    def run(self, datasets, reader_collector_pairs):
        self._begin()
        loop = self._configure(datasets, reader_collector_pairs)
        self._run(loop)
        self._end()

    def _begin(self):
        self.parallel.begin()

    def _configure(self, datasets, reader_collector_pairs):
        reader_top = alphatwirl.loop.ReaderComposite()
        collector_top = alphatwirl.loop.CollectorComposite(self.parallel.progressMonitor.createReporter())
        for r, c in reader_collector_pairs:
            reader_top.add(r)
            collector_top.add(c)
        eventLoopRunner = alphatwirl.loop.MPEventLoopRunner(self.parallel.communicationChannel)
        eventBuilderConfigMaker = EventBuilderConfigMaker()
        datasetIntoEventBuildersSplitter = alphatwirl.loop.DatasetIntoEventBuildersSplitter(
            EventBuilder = EventBuilder,
            eventBuilderConfigMaker = eventBuilderConfigMaker,
            maxEvents = self.max_events_per_dataset,
            maxEventsPerRun = self.max_events_per_process,
            maxFiles = self.max_files_per_dataset,
            maxFilesPerRun = self.max_files_per_process
        )
        eventReader = alphatwirl.loop.EventReader(
            eventLoopRunner = eventLoopRunner,
            reader = reader_top,
            collector = collector_top,
            split_into_build_events = datasetIntoEventBuildersSplitter
        )
        loop = DatasetLoop(datasets = datasets, reader = eventReader)
        return loop

    def _run(self, loop):
        if not self.profile:
            loop()
        else:
            profile_func(func = loop, profile_out_path = self.profile_out_path)

    def _end(self):
        self.parallel.end()

##__________________________________________________________________||
class DatasetLoop(object):

    def __init__(self, datasets, reader):
        self.datasets = datasets
        self.reader = reader

    def __call__(self):
        self.reader.begin()
        for dataset in self.datasets:
            self.reader.read(dataset)
        return self.reader.end()

##__________________________________________________________________||
class Dataset(object):
    def __init__(self, name, files):
        self.name = name
        self.files = files

    def __repr__(self):
        return '{}(name = {!r}, files = {!r}'.format(
            self.__class__.__name__,
            self.name,
            self.files
        )

##__________________________________________________________________||
class Events(object):
    def __init__(self, paths, maxEvents = -1, start = 0):

        if start < 0:
            raise ValueError("start must be greater than or equal to zero: {} is given".format(start))

        self.edm_event = EDMEvents(paths)
        # https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/FWLite/python/__init__.py#L457

        nevents_in_dataset = self.edm_event.size()
        start = min(nevents_in_dataset, start)
        if maxEvents > -1:
            self.nEvents = min(nevents_in_dataset - start, maxEvents)
        else:
            self.nEvents = nevents_in_dataset - start
        self.start = start
        self.iEvent = -1

    def __iter__(self):
        for self.iEvent in xrange(self.nEvents):
            self.edm_event.to(self.start + self.iEvent)
            yield self
        self.iEvent = -1

##__________________________________________________________________||
EventBuilderConfig = collections.namedtuple(
    'EventBuilderConfig',
    'inputPaths maxEvents start dataset name'
)

##__________________________________________________________________||
class EventBuilderConfigMaker(object):
    def __init__(self):
        pass

    def create_config_for(self, dataset, files, start, length):
        config = EventBuilderConfig(
            inputPaths = files,
            maxEvents = length,
            start = start,
            dataset = dataset, # for scribblers
            name = dataset.name # for the progress report writer
        )
        return config

    def file_list_in(self, dataset, maxFiles):
        if maxFiles < 0:
            return dataset.files
        return dataset.files[:min(maxFiles, len(dataset.files))]

    def nevents_in_file(self, path):
        edm_event = EDMEvents([path])
        return edm_event.size()

##__________________________________________________________________||
class EventBuilder(object):
    def __init__(self, config):
        self.config = config

    def __call__(self):
        events = Events(
            paths = self.config.inputPaths,
            maxEvents = self.config.maxEvents,
            start = self.config.start
        )
        events.config = self.config
        events.dataset = self.config.dataset.name
        return events

##__________________________________________________________________||
def loadLibraries():
    argv_org = list(sys.argv)
    sys.argv = [e for e in sys.argv if e != '-h']
    ROOT.gSystem.Load("libFWCoreFWLite")
    ROOT.AutoLibraryLoader.enable()
    ROOT.gSystem.Load("libDataFormatsFWLite")
    ROOT.gSystem.Load("libDataFormatsPatCandidates")
    sys.argv = argv_org

##__________________________________________________________________||
loadLibraries()
from DataFormats.FWLite import Handle
from DataFormats.FWLite import Events as EDMEvents
# https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/FWLite/python/__init__.py

##__________________________________________________________________||
