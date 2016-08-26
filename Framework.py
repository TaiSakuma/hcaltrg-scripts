# Tai Sakuma <sakuma@cern.ch>
import sys
import ROOT
import AlphaTwirl

ROOT.gROOT.SetBatch(1)

##__________________________________________________________________||
class Framework(object):
    def __init__(self, quiet = False, process = 8,
                 max_events_per_dataset = -1,
                 max_events_per_process = -1
    ):
        self.progressMonitor, self.communicationChannel = AlphaTwirl.Configure.build_progressMonitor_communicationChannel(quiet = quiet, processes = process)
        self.max_events_per_dataset = max_events_per_dataset
        self.max_events_per_process = max_events_per_process

    def run(self, datasets, reader_collector_pairs):
        self._begin()
        reader_top = AlphaTwirl.Loop.ReaderComposite()
        collector_top = AlphaTwirl.Loop.CollectorComposite(self.progressMonitor.createReporter())
        for r, c in reader_collector_pairs:
            reader_top.add(r)
            collector_top.add(c)
        eventLoopRunner = AlphaTwirl.Loop.MPEventLoopRunner(self.communicationChannel)
        eventBuilder = EventBuilder(maxEvents = self.max_events_per_dataset)
        eventReader = AlphaTwirl.Loop.EventReader(
            eventBuilder = eventBuilder,
            eventLoopRunner = eventLoopRunner,
            reader = reader_top,
            collector = collector_top,
            maxEventsPerRun = self.max_events_per_process
        )
        eventReader.begin()
        for dataset in datasets:
            eventReader.read(dataset)
        eventReader.end()
        self._end()

    def _begin(self):
        self.progressMonitor.begin()
        self.communicationChannel.begin()

    def _end(self):
        self.progressMonitor.end()
        self.communicationChannel.end()

##__________________________________________________________________||
class Dataset(object):
    def __init__(self, name, files):
        self.name = name
        self.files = files

##__________________________________________________________________||
class Events(object):
    def __init__(self, dataset, maxEvents = -1, start = 0):

        if start < 0:
            raise ValueError("start must be greater than or equal to zero: {} is given".format(start))

        self.edm_event = EDMEvents(dataset.files)
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
class EventBuilder(object):
    def __init__(self, maxEvents = -1):
        self.maxEvents = maxEvents

    def getNumberOfEventsInDataset(self, dataset):
        edm_event = EDMEvents(dataset.files)
        return self._minimumPositiveValue([self.maxEvents, edm_event.size()])

    def build(self, dataset, start = 0, nEvents = -1):
        maxEvents = self._minimumPositiveValue([self.maxEvents, nEvents])
        return Events(dataset, maxEvents, start)

    def _minimumPositiveValue(self, vals):
        vals = [v for v in vals if v >= 0]
        if not vals: return -1
        return min(vals)
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
