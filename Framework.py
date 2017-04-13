# Tai Sakuma <sakuma@cern.ch>
import sys
import collections

import ROOT

import alphatwirl

ROOT.gROOT.SetBatch(1)

##__________________________________________________________________||
class Framework(object):
    def __init__(self, quiet = False,
                 process = 8,
                 max_events_per_dataset = -1,
                 max_events_per_process = -1,
                 max_files_per_dataset = -1,
                 profile = False,
                 profile_out_path = None
    ):
        self.progressMonitor, self.communicationChannel = alphatwirl.configure.build_progressMonitor_communicationChannel(quiet = quiet, processes = process)
        self.max_events_per_dataset = max_events_per_dataset
        self.max_events_per_process = max_events_per_process
        self.max_files_per_dataset = max_files_per_dataset
        self.profile = profile
        self.profile_out_path = profile_out_path

    def run(self, datasets, reader_collector_pairs):
        self._begin()
        loop = self._configure(datasets, reader_collector_pairs)
        self._run(loop)
        self._end()

    def _begin(self):
        self.progressMonitor.begin()
        self.communicationChannel.begin()

    def _configure(self, datasets, reader_collector_pairs):
        reader_top = alphatwirl.loop.ReaderComposite()
        collector_top = alphatwirl.loop.CollectorComposite(self.progressMonitor.createReporter())
        for r, c in reader_collector_pairs:
            reader_top.add(r)
            collector_top.add(c)
        eventLoopRunner = alphatwirl.loop.MPEventLoopRunner(self.communicationChannel)
        eventBuilderConfigMaker = EventBuilderConfigMaker()
        datasetIntoEventBuildersSplitter = alphatwirl.loop.DatasetIntoEventBuildersSplitter(
            EventBuilder = EventBuilder,
            eventBuilderConfigMaker = eventBuilderConfigMaker,
            maxEvents = self.max_events_per_dataset,
            maxEventsPerRun = self.max_events_per_process,
            maxFiles = self.max_files_per_dataset
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
        self.progressMonitor.end()
        self.communicationChannel.end()

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
    def __init__(self, path, maxEvents = -1, start = 0):

        if start < 0:
            raise ValueError("start must be greater than or equal to zero: {} is given".format(start))

        self.edm_event = EDMEvents([path])
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
EventBuilderConfig = collections.namedtuple('EventBuilderConfig', 'inputPath maxEvents start dataset name')

##__________________________________________________________________||
class EventBuilderConfigMaker(object):
    def __init__(self):
        pass

    def create_config_for(self, dataset, file_, start, length):
        config = EventBuilderConfig(
            inputPath = file_,
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

    def file_nevents_list_for(self, dataset, maxEvents, maxFiles):
        files = self.file_list_in(dataset, maxFiles = maxFiles)
        totalEvents = 0
        ret = [ ]
        for f in files:
            if 0 <= maxEvents <= totalEvents:
                return ret
            n = self.nevents_in_file(f)
            ret.append((f, n))
            totalEvents += n
        return ret

    def nevents_in_file(self, path):
        edm_event = EDMEvents([path])
        return edm_event.size()

##__________________________________________________________________||
class EventBuilder(object):
    def __init__(self, config):
        self.config = config

    def __call__(self):
        events = Events(
            path = self.config.inputPath,
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
