# Tai Sakuma <sakuma@cern.ch>
import sys
import ROOT
import AlphaTwirl

ROOT.gROOT.SetBatch(1)

##__________________________________________________________________||
class Framework(object):
    def __init__(self, quiet = False, process = 8):
        self.progressMonitor, self.communicationChannel = AlphaTwirl.Configure.build_progressMonitor_communicationChannel(quiet = quiet, processes = process)


    def run(self, dataset, reader_collector_pairs):
        self._begin()
        reader_top = AlphaTwirl.Loop.ReaderComposite()
        collector_top = AlphaTwirl.Loop.CollectorComposite(self.progressMonitor.createReporter())
        for r, c in reader_collector_pairs:
            reader_top.add(r)
            collector_top.add(c)
        eventLoopRunner = AlphaTwirl.Loop.MPEventLoopRunner(self.communicationChannel)
        eventBuilder = EventBuilder()
        eventReader = AlphaTwirl.Loop.EventReader(eventBuilder, eventLoopRunner, reader_top, collector_top)
        eventReader.begin()
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
    def __init__(self, dataset):
        self._edmEvents = EDMEvents(dataset.files)
        self.iEvent = -1
        self.nEvents = self._edmEvents.size()

    def __iter__(self):
        it = iter(self._edmEvents)
        while True:
            try:
                self.edm_event = next(it)
                self.iEvent = self.edm_event._eventCounts
                self.nEvents = self.edm_event.size()
                yield self
            except StopIteration:
                break

##__________________________________________________________________||
class EventBuilder(object):
    def __init__(self):
        pass

    def build(self, dataset, start = 0, nEvents = -1):
        return Events(dataset)
        # return EDMEvents(dataset.files)

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
