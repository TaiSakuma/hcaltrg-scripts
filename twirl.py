#!/usr/bin/env python
# Tai Sakuma <sakuma@cern.ch>
import os, sys
import argparse

import ROOT

import alphatwirl
import Framework
import Scribbler

ROOT.gROOT.SetBatch(1)

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument("--input-files", default = [ ], nargs = '*', help = "list of input files")
parser.add_argument("--dataset-names", default = [ ], nargs = '*', help = "list of data set names")
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
    NullCollector = alphatwirl.loop.NullCollector
    reader_collector_pairs.extend([
        (Scribbler.EventAuxiliary(), NullCollector()),
        (Scribbler.MET(),            NullCollector()),
        (Scribbler.GenParticle(),    NullCollector()),
        (Scribbler.HFPreRecHit(),    NullCollector()),
        (Scribbler.HFPreRecHit_QIE10_energy_th(min_energy = 3),    NullCollector()),
        (Scribbler.HFPreRecHitEtaPhi(), NullCollector()),
        (Scribbler.QIE10MergedDepth(), NullCollector()),
        (Scribbler.GenMatching(), NullCollector()),
        # (Scribbler.QIE10Ag(),        NullCollector()),
        # (Scribbler.Scratch(),        NullCollector()),
        ])

    #
    # configure tables
    #
    Binning = alphatwirl.binning.Binning
    Echo = alphatwirl.binning.Echo
    Round = alphatwirl.binning.Round
    RoundLog = alphatwirl.binning.RoundLog
    Combine = alphatwirl.binning.Combine
    echo = Echo(nextFunc = None)
    echoNextPlusOne = Echo()
    tblcfg = [
        dict(keyAttrNames = ('run', ), binnings = (echo, )),
        dict(keyAttrNames = ('lumi', ), binnings = (echo, )),
        dict(keyAttrNames = ('eventId', ), binnings = (echo, )),
        dict(keyAttrNames = ('pfMet', ), binnings = (Round(10, 0), )),
        dict(keyAttrNames = ('genParticle_pdgId', ), keyIndices = ('*', ), binnings = (echoNextPlusOne, ), keyOutColumnNames = ('gen_pdg', )),
        dict(keyAttrNames = ('genParticle_eta', ), keyIndices = ('*', ), binnings = (Round(0.2, 0), ), keyOutColumnNames = ('gen_eta', )),
        dict(keyAttrNames = ('genParticle_pdgId', 'genParticle_eta'), keyIndices = ('(*)', '\\1'), binnings = (echoNextPlusOne, Round(0.2, 0)), keyOutColumnNames = ('gen_pdg', 'gen_eta')),
        dict(keyAttrNames = ('genParticle_phi', ), keyIndices = ('*', ), binnings = (Round(0.0314159265*5, 0), ), keyOutColumnNames = ('gen_phi', )),
        dict(keyAttrNames = ('genParticle_energy', ), keyIndices = ('*', ), binnings = (Round(0.1, 0), ), keyOutColumnNames = ('gen_energy', )),
        dict(
            keyAttrNames = ('hfrechit_ieta', 'hfrechit_iphi', 'hfrechit_depth', 'hfrechit_QIE10_index'),
            keyIndices = ('(*)', '\\1', '\\1', '\\1'),
            binnings = (echo, echo, echo, echo),
            valAttrNames = ('hfrechit_QIE10_energy', ),
            valIndices = ('\\1', ),
            keyOutColumnNames = ('ieta', 'iphi', 'depth', 'idxQIE10'),
            valOutColumnNames = ('energy', ),
            summaryClass = alphatwirl.summary.Sum,
        ),
        dict(keyAttrNames = ('hfrechit_depth', 'hfrechit_QIE10_index', 'hfrechit_QIE10_charge'),      keyIndices = ('(*)', '\\1', '\\1'), binnings = (echo, echo, Round(0.1, 0)), keyOutColumnNames = ('depth', 'idxQIE10', 'QIE10_charge')),
        dict(keyAttrNames = ('hfrechit_depth', 'hfrechit_QIE10_index', 'hfrechit_QIE10_energy'),      keyIndices = ('(*)', '\\1', '\\1'), binnings = (echo, echo, Round(0.1, 0)), keyOutColumnNames = ('depth', 'idxQIE10', 'QIE10_energy')),
        dict(keyAttrNames = ('hfrechit_depth', 'hfrechit_QIE10_index', 'hfrechit_QIE10_energy_th'),   keyIndices = ('(*)', '\\1', '\\1'), binnings = (echo, echo, Round(0.1, 0, valid = greater_than_zero)), keyOutColumnNames = ('depth', 'idxQIE10', 'QIE10_energy_th')),
        dict(keyAttrNames = ('hfrechit_depth', 'hfrechit_QIE10_index', 'hfrechit_QIE10_timeRising'),  keyIndices = ('(*)', '\\1', '\\1'), binnings = (echo, echo, Round(0.1, 0)), keyOutColumnNames = ('depth', 'idxQIE10', 'QIE10_timeRising')),
        dict(keyAttrNames = ('hfrechit_depth', 'hfrechit_QIE10_index', 'hfrechit_QIE10_timeFalling'), keyIndices = ('(*)', '\\1', '\\1'), binnings = (echo, echo, Round(0.1, 0)), keyOutColumnNames = ('depth', 'idxQIE10', 'QIE10_timeFalling')),
        dict(keyAttrNames = ('hfrechit_depth', 'hfrechit_QIE10_index', 'hfrechit_QIE10_nRaw'),        keyIndices = ('(*)', '\\1', '\\1'), binnings = (echo, echo, Round(1, 0)  ), keyOutColumnNames = ('depth', 'idxQIE10', 'QIE10_nRaw')),
        dict(keyAttrNames = ('hfrechit_depth', 'hfrechit_QIE10_index', 'hfrechit_QIE10_soi'),         keyIndices = ('(*)', '\\1', '\\1'), binnings = (echo, echo, Round(1, 0)  ), keyOutColumnNames = ('depth', 'idxQIE10', 'QIE10_soi')),
        dict(keyAttrNames = ('QIE10MergedDepth_energy_ratio', ), keyIndices = ('*', ), binnings = (Round(0.5, 0, valid = greater_than_zero), ), keyOutColumnNames = ('QIE10_energy_ratio', )),
        dict(keyAttrNames = ('GenMatchedSummed_energy_depth1', ), keyIndices = ('*', ), binnings = (Round(0.1, 0, valid = greater_than_zero), ), keyOutColumnNames = ('matched_energy_depth1', )),
        dict(keyAttrNames = ('GenMatchedSummed_energy_depth2', ), keyIndices = ('*', ), binnings = (Round(0.1, 0, valid = greater_than_zero), ), keyOutColumnNames = ('matched_energy_depth2', )),
        dict(keyAttrNames = ('GenMatchedSummed_energy_ratio', ), keyIndices = ('*', ), binnings = (Round(0.5, 0, valid = greater_than_zero), ), keyOutColumnNames = ('matched_energy_ratio', )),
        dict(keyAttrNames = ('GenMatchedSummed_qie_index', 'GenMatchedSummed_energy_depth1', ), keyIndices = (None, '*'), binnings = (echo, Round(0.1, 0, valid = greater_than_zero)), keyOutColumnNames = ('idxQIE10', 'matched_energy_depth1')),
        dict(keyAttrNames = ('GenMatchedSummed_qie_index', 'GenMatchedSummed_energy_depth2', ), keyIndices = (None, '*'), binnings = (echo, Round(0.1, 0, valid = greater_than_zero)), keyOutColumnNames = ('idxQIE10', 'matched_energy_depth2')),
        dict(keyAttrNames = ('GenMatchedSummed_qie_index', 'GenMatchedSummed_energy_ratio', ),  keyIndices = (None, '*'), binnings = (echo, Round(0.5, 0, valid = greater_than_zero)), keyOutColumnNames = ('idxQIE10', 'matched_energy_ratio')),
        dict(keyAttrNames = ('GenMatchedSummedDepthEnergy_depth', 'GenMatchedSummedDepthEnergy_qie_index', 'GenMatchedSummedDepthEnergy_energy'),      keyIndices = ('(*)', '\\1', '\\1'), binnings = (echo, echo, Round(0.1, 0)), keyOutColumnNames = ('depth', 'idxQIE10', 'energy_matched_summed')),
    ]

    # complete table configs
    tableConfigCompleter = alphatwirl.configure.TableConfigCompleter(
        defaultSummaryClass = alphatwirl.summary.Count,
        defaultOutDir = args.outdir,
        createOutFileName = alphatwirl.configure.TableFileNameComposer2()
    )
    tblcfg = [tableConfigCompleter.complete(c) for c in tblcfg]

    # do not recreate tables that already exist unless the force option is used
    if not args.force:
        tblcfg = [c for c in tblcfg if c['outFile'] and not os.path.exists(c['outFilePath'])]

    reader_collector_pairs.extend(
        [alphatwirl.configure.build_counter_collector_pair(c) for c in tblcfg]
    )

    #
    # configure data sets
    #
    dataset_names = args.dataset_names if args.dataset_names else args.input_files
    datasets = [Framework.Dataset(n, [f]) for n, f in zip(dataset_names, args.input_files)]

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
        datasets = datasets,
        reader_collector_pairs = reader_collector_pairs
    )

##__________________________________________________________________||
def greater_than_zero(x): return x > 0

##__________________________________________________________________||
if __name__ == '__main__':
    main()
