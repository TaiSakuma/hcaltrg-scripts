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

parser.add_argument('-n', '--nevents', default = -1, type = int, help = 'maximum number of events to process for each component')
parser.add_argument('--max-events-per-process', default = -1, type = int, help = 'maximum number of events per process')
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
        (Scribbler.GenParticle(),    NullCollector()),
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
    echoNextPlusOne = Echo()
    tblcfg = [
        dict(keyAttrNames = ('run', ), binnings = (echo, )),
        dict(keyAttrNames = ('lumi', ), binnings = (echo, )),
        dict(keyAttrNames = ('eventId', ), binnings = (echo, )),
        dict(keyAttrNames = ('pfMet', ), binnings = (Round(10, 0), )),
        dict(keyAttrNames = ('genParticle_pdgId', ), keyIndices = ('*', ), binnings = (echoNextPlusOne, ), keyOutColumnNames = ('gen_pdg', )),
        dict(keyAttrNames = ('genParticle_eta', ), keyIndices = ('*', ), binnings = (Round(0.1, 0), ), keyOutColumnNames = ('gen_eta', )),
        dict(keyAttrNames = ('genParticle_pdgId', 'genParticle_eta'), keyIndices = ('(*)', '\\1'), binnings = (echoNextPlusOne, Round(0.1, 0)), keyOutColumnNames = ('gen_pdg', 'gen_eta')),
        dict(keyAttrNames = ('genParticle_phi', ), keyIndices = ('*', ), binnings = (Round(0.0314159265*3, 0), ), keyOutColumnNames = ('gen_phi', )),
        dict(keyAttrNames = ('genParticle_energy', ), keyIndices = ('*', ), binnings = (Round(0.1, 0), ), keyOutColumnNames = ('gen_energy', )),
        dict(
            keyAttrNames = ('hfrechit_ieta', 'hfrechit_iphi', 'hfrechit_QIE10_index'),
            keyIndices = ('(*)', '\\1', '\\1'),
            binnings = (echo, echo, echo),
            valAttrNames = ('hfrechit_QIE10_energy', ),
            valIndices = ('\\1', ),
            keyOutColumnNames = ('ieta', 'iphi', 'idxQIE10'),
            valOutColumnNames = ('energy', ),
            summaryClass = AlphaTwirl.Summary.Sum,
            outFile = True,
        ),
    ]

    # complete table configs
    tableConfigCompleter = AlphaTwirl.Configure.TableConfigCompleter(
        defaultSummaryClass = AlphaTwirl.Summary.Count,
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
    fw =  Framework.Framework(
        quiet = args.quiet,
        process = args.process,
        max_events_per_dataset = args.nevents,
        max_events_per_process = args.max_events_per_process
    )
    fw.run(
        dataset = dataset,
        reader_collector_pairs = reader_collector_pairs
    )

##__________________________________________________________________||
if __name__ == '__main__':
    main()
