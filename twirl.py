#!/usr/bin/env python
# Tai Sakuma <sakuma@cern.ch>
import os, sys
import argparse

import ROOT

import AlphaTwirl
import Framework
import Scribbler

ROOT.gROOT.SetBatch(1)

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", help = "the path to the input file")
parser.add_argument("-p", "--process", default = 1, type = int, help = "number of processes to run in parallel")
parser.add_argument('-o', '--outdir', default = os.path.join('tbl', 'out'))
parser.add_argument('-q', '--quiet', action = 'store_true', default = False, help = 'quiet mode')
parser.add_argument('--force', action = 'store_true', default = False, help = 'recreate all output files')
args = parser.parse_args()

##__________________________________________________________________||
def main():

    reader_collector_pairs = [ ]

    #
    # configure scribblers
    #
    NullCollector = AlphaTwirl.Loop.NullCollector
    reader_collector_pairs.extend([
        (Scribbler.EventAuxiliary(), NullCollector()),
        (Scribbler.MET(),            NullCollector()),
        (Scribbler.HFPreRecHit(),    NullCollector()),
        # (Scribbler.Scratch(),        NullCollector()),
        ])

    #
    # configure tables
    #
    Binning = AlphaTwirl.Binning.Binning
    Echo = AlphaTwirl.Binning.Echo
    Round = AlphaTwirl.Binning.Round
    RoundLog = AlphaTwirl.Binning.RoundLog
    Combine = AlphaTwirl.Binning.Combine
    echo = Echo(nextFunc = None)
    tblcfg = [
        dict(branchNames = ('run', ), binnings = (echo, )),
        dict(branchNames = ('lumi', ), binnings = (echo, )),
        dict(branchNames = ('eventId', ), binnings = (echo, )),
        dict(branchNames = ('pfMet', ), binnings = (Round(10, 0), )),
        # dict(
        #     branchNames = ('hfrechit_QIE10_energy', ),
        #     binnings = (Round(0.1, 0), ),
        #     indices = ('(*)', ),
        #     outColumnNames = ('energy', ),
        #
        # ),
        # dict(
        #     branchNames = ('hfrechit_ieta', 'hfrechit_iphi', 'hfrechit_QIE10_index', 'hfrechit_QIE10_energy'),
        #     binnings = (echo, echo, echo, Round(0.1, 0)),
        #     indices = ('(*)', '\\1', '\\1', '\\1'),
        #     outColumnNames = ('ieta', 'iphi', 'idxQIE10', 'energy'),
        # ),
    ]

    # complete table configs
    tableConfigCompleter = AlphaTwirl.Configure.TableConfigCompleter(
        defaultCountsClass = AlphaTwirl.Summary.Count,
        defaultOutDir = args.outdir
    )
    tblcfg = [tableConfigCompleter.complete(c) for c in tblcfg]

    # do not recreate tables that already exist unless the force option is used
    if not args.force:
        tblcfg = [c for c in tblcfg if c['outFile'] and not os.path.exists(c['outFilePath'])]

    reader_collector_pairs.extend(
        [AlphaTwirl.Configure.build_counter_collector_pair(c) for c in tblcfg]
    )

    #
    # add custom readers
    #
    reader_collector_pairs.extend(
        build_custom_reader_collector_pairs()
    )

    #
    # configure data sets
    #
    dataset = Framework.Dataset('root3', [args.input])

    #
    # run
    #
    fw =  Framework.Framework(quiet = args.quiet, process = args.process)
    fw.run(
        dataset = dataset,
        reader_collector_pairs = reader_collector_pairs
    )

##__________________________________________________________________||
def build_custom_reader_collector_pairs():
    ret = [ ]

    Binning = AlphaTwirl.Binning.Binning
    Echo = AlphaTwirl.Binning.Echo
    Round = AlphaTwirl.Binning.Round
    RoundLog = AlphaTwirl.Binning.RoundLog
    Combine = AlphaTwirl.Binning.Combine
    echo = Echo(nextFunc = None)
    tblcfg = [
        dict(
            keyAttrNames = ('hfrechit_ieta', 'hfrechit_iphi', 'hfrechit_QIE10_index'),
            keyIndices = ('(*)', '\\1', '\\1'),
            binnings = (echo, echo, echo),
            valAttrNames = ('hfrechit_QIE10_energy', ),
            valIndices = ('\\1', ),
            keyOutColumnNames = ('ieta', 'iphi', 'idxQIE10'),
            valOutColumnNames = ('energy', ),
            countsClass = AlphaTwirl.Summary.Sum,
            outFilePath = os.path.join(args.outdir, 'tbl_custom_hfprefechit_energy.txt'),
            outFile = True,
        ),
    ]
    ret.extend([custom_build_counter_collector_pair(c) for c in tblcfg])

    # outFilePath = os.path.join(args.outdir, 'tbl_custom_hfprefechit_energy.txt')
    # resultsCombinationMethod = CombineIntoList(
    #     keyNames = ('ieta', 'iphi'),
    #     valNames = ('energy', )
    # )
    # deliveryMethod = WriteListToFile(outFilePath)
    # collector = Collector(resultsCombinationMethod, deliveryMethod)
    # ret.append([HFPreRecHit(), collector])

    return ret

##__________________________________________________________________||
import collections
class HFPreRecHit(object):
    def __init__(self):
        self.outColumnValName = ('ieta', 'iphi', 'idxQIE10')
        self.outColumnValName = 'energy'

        self._counts = { }

    def begin(self, event): pass

    def event(self, event):
        for c in zip(
            event.hfrechit_ieta,
            event.hfrechit_iphi,
            event.hfrechit_QIE10_index,
            event.hfrechit_QIE10_energy,
            ):
            key = c[0:3]
            val = c[3]
            if key not in self._counts:
                self._counts[key] = collections.OrderedDict(((self.outColumnValName, 0.0), ))
            self._counts[key][self.outColumnValName] += val

    def end(self): pass

    def copy_from(self, src):
        self._counts.clear()
        self._counts.update(src._counts)

    def results(self):
        return self._counts

##__________________________________________________________________||
from AlphaTwirl.Summary import NextKeyComposer, KeyValueComposer
from AlphaTwirl.CombineIntoList import CombineIntoList
from AlphaTwirl.WriteListToFile import WriteListToFile
from AlphaTwirl.Loop import Collector
def custom_build_counter_collector_pair(tblcfg):
    keyValComposer = KeyValueComposer(
        keyAttrNames = tblcfg['keyAttrNames'],
        binnings = tblcfg['binnings'],
        keyIndices = tblcfg['keyIndices'],
        valAttrNames = tblcfg['valAttrNames'],
        valIndices = tblcfg['valIndices']
    )
    nextKeyComposer = NextKeyComposer(tblcfg['binnings'])
    summarizer = AlphaTwirl.Summary.Summarizer(
        keyValComposer = keyValComposer,
        summary = tblcfg['countsClass'](),
        nextKeyComposer = nextKeyComposer,
    )
    resultsCombinationMethod = CombineIntoList(
        keyNames = tblcfg['keyOutColumnNames'],
        valNames = tblcfg['valOutColumnNames']
    )
    deliveryMethod = WriteListToFile(tblcfg['outFilePath']) if tblcfg['outFile'] else None
    collector = Collector(resultsCombinationMethod, deliveryMethod)
    return summarizer, collector

##__________________________________________________________________||
if __name__ == '__main__':
    main()
