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
    NullCollector = AlphaTwirl.EventReader.NullCollector
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
        dict(
            branchNames = ('hfrechit_QIE10_energy', ),
            binnings = (Round(0.1, 0), ),
            indices = ('(*)', ),
            outColumnNames = ('energy', ),

        ),
        dict(
            branchNames = ('hfrechit_ieta', 'hfrechit_iphi', 'hfrechit_QIE10_index', 'hfrechit_QIE10_energy'),
            binnings = (echo, echo, echo, Round(0.1, 0)),
            indices = ('(*)', '\\1', '\\1', '\\1'),
            outColumnNames = ('ieta', 'iphi', 'idxQIE10', 'energy'),

        ),
    ]

    # complete table configs
    tableConfigCompleter = AlphaTwirl.Configure.TableConfigCompleter(
        defaultCountsClass = AlphaTwirl.Counter.Counts,
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
if __name__ == '__main__':
    main()
