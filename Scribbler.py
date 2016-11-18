# Tai Sakuma <sakuma@cern.ch>
import os
import pandas as pd
import numpy as np

##__________________________________________________________________||
class EventAuxiliary(object):
    # https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/Provenance/interface/EventAuxiliary.h

    def begin(self, event):
        self.run = [ ]
        self.lumi = [ ]
        self.eventId = [ ]
        self._attach_to_event(event)

    def _attach_to_event(self, event):
        event.run = self.run
        event.lumi = self.lumi
        event.eventId = self.eventId

    def event(self, event):
        self._attach_to_event(event)

        eventAuxiliary = event.edm_event.eventAuxiliary()
        self.run[:] = [eventAuxiliary.run()]
        self.lumi[:] = [eventAuxiliary.luminosityBlock()]
        self.eventId[:] = [eventAuxiliary.event()]

##__________________________________________________________________||
class MET(object):
    def begin(self, event):
        self.pfMet = [ ]
        self._attach_to_event(event)

        self.handlePFMETs = Handle("std::vector<reco::PFMET>")

    def _attach_to_event(self, event):
        event.pfMet = self.pfMet

    def event(self, event):
        self._attach_to_event(event)

        edm_event = event.edm_event

        edm_event.getByLabel("pfMet", self.handlePFMETs)
        met = self.handlePFMETs.product().front()
        self.pfMet[:] = [met.pt()]

    def end(self):
        self.handlePFMETs = None

##__________________________________________________________________||
class GenParticle(object):
    def begin(self, event):
        self.nGenParticles = [ ]
        self.genParticle_pdgId = [ ]
        self.genParticle_eta = [ ]
        self.genParticle_phi = [ ]
        self.genParticle_energy = [ ]
        self._attach_to_event(event)

        self.handleGenParticles = Handle("std::vector<reco::GenParticle>")

    def _attach_to_event(self, event):
        event.nGenParticles = self.nGenParticles
        event.genParticle_pdgId = self.genParticle_pdgId
        event.genParticle_eta = self.genParticle_eta
        event.genParticle_phi = self.genParticle_phi
        event.genParticle_energy = self.genParticle_energy

    def event(self, event):
        self._attach_to_event(event)

        edm_event = event.edm_event

        edm_event.getByLabel("genParticles", self.handleGenParticles)
        genparts = self.handleGenParticles.product()
        self.nGenParticles[:] = [genparts.size()]
        self.genParticle_pdgId[:] = [e.pdgId() for e in genparts]
        self.genParticle_eta[:] = [e.eta() for e in genparts]
        self.genParticle_phi[:] = [e.phi() for e in genparts]
        self.genParticle_energy[:] = [e.energy() for e in genparts]

    def end(self):
        self.handleGenParticles = None

##__________________________________________________________________||
class HFPreRecHit(object):
    def begin(self, event):
        self.hfrechit_ieta = [ ]
        self.hfrechit_iphi = [ ]
        self.hfrechit_depth = [ ]
        self.hfrechit_QIE10_index = [ ]
        self.hfrechit_QIE10_charge = [ ]
        self.hfrechit_QIE10_energy = [ ]
        self.hfrechit_QIE10_timeRising = [ ]
        self.hfrechit_QIE10_timeFalling = [ ]
        self.hfrechit_QIE10_nRaw = [ ]
        self.hfrechit_QIE10_soi = [ ]
        self._attach_to_event(event)

        self.handleHFPreRecHit = Handle("edm::SortedCollection<HFPreRecHit,edm::StrictWeakOrdering<HFPreRecHit> >")
        # SortedCollection: https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/Common/interface/SortedCollection.h
        # HFPreRecHit: https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/HcalRecHit/interface/HFPreRecHit.h

    def _attach_to_event(self, event):
        event.hfrechit_ieta = self.hfrechit_ieta
        event.hfrechit_iphi = self.hfrechit_iphi
        event.hfrechit_depth = self.hfrechit_depth
        event.hfrechit_QIE10_index = self.hfrechit_QIE10_index
        event.hfrechit_QIE10_charge = self.hfrechit_QIE10_charge
        event.hfrechit_QIE10_energy = self.hfrechit_QIE10_energy
        event.hfrechit_QIE10_timeRising = self.hfrechit_QIE10_timeRising
        event.hfrechit_QIE10_timeFalling = self.hfrechit_QIE10_timeFalling
        event.hfrechit_QIE10_nRaw = self.hfrechit_QIE10_nRaw
        event.hfrechit_QIE10_soi = self.hfrechit_QIE10_soi

    def event(self, event):
        self._attach_to_event(event)

        edm_event = event.edm_event

        edm_event.getByLabel('hfprereco', self.handleHFPreRecHit)
        hfPreRecoHits = self.handleHFPreRecHit.product()

        self.hfrechit_ieta[:] = [h.id().ieta() for h in hfPreRecoHits]*2
        self.hfrechit_iphi[:] = [h.id().iphi() for h in hfPreRecoHits]*2
        self.hfrechit_depth[:] = [h.id().depth() for h in hfPreRecoHits]*2
        self.hfrechit_QIE10_index[:] = [0]*len(hfPreRecoHits) + [1]*len(hfPreRecoHits)

        HFQIE10Infos = [h.getHFQIE10Info(i) for i in (0, 1) for h in hfPreRecoHits]
        self.hfrechit_QIE10_charge[:] = [i.charge() for i in HFQIE10Infos]
        self.hfrechit_QIE10_energy[:] = [i.energy() for i in HFQIE10Infos]
        self.hfrechit_QIE10_timeRising[:] = [i.timeRising() for i in HFQIE10Infos]
        self.hfrechit_QIE10_timeFalling[:] = [i.timeFalling() for i in HFQIE10Infos]
        self.hfrechit_QIE10_nRaw[:] = [i.nRaw() for i in HFQIE10Infos]
        self.hfrechit_QIE10_soi[:] = [i.soi() for i in HFQIE10Infos]

    def end(self):
        self.handleHFPreRecHit = None

##__________________________________________________________________||
class HFPreRecHitEtaPhi(object):
    def begin(self, event):
        self.hfrechit_eta = [ ]
        self.hfrechit_phi = [ ]
        self._attach_to_event(event)

        this_dir = os.path.realpath(os.path.dirname(__file__))
        tbl_dir = os.path.join(this_dir, 'tbl')
        tbl_path = os.path.join(tbl_dir, 'tbl_HF_ieta_iphi_eta_phi.txt')
        self.tbl_eta_phi = pd.read_table(tbl_path, delim_whitespace = True)

    def _attach_to_event(self, event):
        event.hfrechit_eta = self.hfrechit_eta
        event.hfrechit_phi = self.hfrechit_phi

    def event(self, event):
        self._attach_to_event(event)

        df = pd.DataFrame({
            'ieta': event.hfrechit_ieta,
            'iphi': event.hfrechit_iphi,
            'hfdepth': event.hfrechit_depth,
        })

        # merge while preserving the order
        # http://stackoverflow.com/questions/20206615/how-can-a-pandas-merge-preserve-order
        res = df.merge(self.tbl_eta_phi, how = 'left')
        # this should preserve the order. at least the following prints True
        # print np.all(res[['ieta', 'iphi', 'hfdepth']] == df[['ieta', 'iphi', 'hfdepth']])

        self.hfrechit_eta[:] = res.eta
        self.hfrechit_phi[:] = res.phi

##__________________________________________________________________||
class HFPreRecHit_QIE10_energy_th(object):
    def __init__(self, min_energy = 3):
        self.min_energy = min_energy

    def begin(self, event):
        self.hfrechit_QIE10_energy_th = [ ]
        self._attach_to_event(event)

    def _attach_to_event(self, event):
        event.hfrechit_QIE10_energy_th = self.hfrechit_QIE10_energy_th

    def event(self, event):
        self._attach_to_event(event)
        self.hfrechit_QIE10_energy_th[:] = [(e if e >= self.min_energy else 0) for e in event.hfrechit_QIE10_energy]

##__________________________________________________________________||
class QIE10MergedDepth(object):
    def begin(self, event):
        self.QIE10MergedDepth_ieta = [ ]
        self.QIE10MergedDepth_iphi = [ ]
        self.QIE10MergedDepth_index = [ ]
        self.QIE10MergedDepth_energy_depth1 = [ ]
        self.QIE10MergedDepth_energy_depth2 = [ ]
        self.QIE10MergedDepth_energy_ratio = [ ]
        self._attach_to_event(event)

    def _attach_to_event(self, event):
        event.QIE10MergedDepth_ieta = self.QIE10MergedDepth_ieta
        event.QIE10MergedDepth_iphi = self.QIE10MergedDepth_iphi
        event.QIE10MergedDepth_index = self.QIE10MergedDepth_index
        event.QIE10MergedDepth_energy_depth1 = self.QIE10MergedDepth_energy_depth1
        event.QIE10MergedDepth_energy_depth2 = self.QIE10MergedDepth_energy_depth2
        event.QIE10MergedDepth_energy_ratio = self.QIE10MergedDepth_energy_ratio

    def event(self, event):
        self._attach_to_event(event)

        df = pd.DataFrame({
            'ieta': event.hfrechit_ieta,
            'iphi': event.hfrechit_iphi,
            'index': event.hfrechit_QIE10_index,
            'depth': event.hfrechit_depth,
            'energy': event.hfrechit_QIE10_energy_th
        })

        df = pd.pivot_table(
            df,
            values = ['energy'],
            index = ['ieta', 'iphi', 'index'],
            columns = ['depth']).reset_index()
        df.columns = ['_'.join(['{}'.format(f) for f in e]).strip('_') for e in df]
        df['energy_ratio'] = np.where(df.energy_2 > 0, df.energy_1/df.energy_2, 0)

        self.QIE10MergedDepth_ieta[:] = df.ieta
        self.QIE10MergedDepth_iphi[:] = df.iphi
        self.QIE10MergedDepth_index[:] = df.index
        self.QIE10MergedDepth_energy_depth1[:] = df.energy_1
        self.QIE10MergedDepth_energy_depth2[:] =  df.energy_2
        self.QIE10MergedDepth_energy_ratio[:] = df.energy_ratio

    def end(self):
        pass

##__________________________________________________________________||
class QIE10Ag(object):
    """This class is outdated, but might become useful for slightly
    different purpose.

    This class was originally made to take the energy ratio of the
    long and short fibers. However, the implementation is wrong. This
    class takes the ratio of energies for different QIE10 indices.

    It turns out that different indices are not for long and short
    fibers. They are probably for two readouts on the same QIE10. If
    this is the case, this class can be reused to take an energy
    average of two readouts.

    """
    def begin(self, event):
        self.QIE10Ag_ieta = [ ]
        self.QIE10Ag_iphi = [ ]
        self.QIE10Ag_energy_ratio = [ ]
        self._attach_to_event(event)

    def _attach_to_event(self, event):
        event.QIE10Ag_ieta = self.QIE10Ag_ieta
        event.QIE10Ag_iphi = self.QIE10Ag_iphi
        event.QIE10Ag_energy_ratio = self.QIE10Ag_energy_ratio

    def event(self, event):
        self._attach_to_event(event)

        len_hfrechit = len(event.hfrechit_QIE10_index)/2
        energy0 = np.array(event.hfrechit_QIE10_energy_th[:len_hfrechit])
        energy1 = np.array(event.hfrechit_QIE10_energy_th[len_hfrechit:])
        ratio = np.where(energy1 > 0, energy0/energy1, 0)
        self.QIE10Ag_ieta[:] = event.hfrechit_ieta[:len_hfrechit]
        self.QIE10Ag_iphi[:] = event.hfrechit_iphi[:len_hfrechit]
        self.QIE10Ag_energy_ratio[:] = ratio

    def end(self):
        pass

##__________________________________________________________________||
class Scratch(object):
    def begin(self, event):
        self._attach_to_event(event)

        self.handleHFPreRecHit = Handle("edm::SortedCollection<HFPreRecHit,edm::StrictWeakOrdering<HFPreRecHit> >")
        # SortedCollection: https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/Common/interface/SortedCollection.h
        # HFPreRecHit: https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/HcalRecHit/interface/HFPreRecHit.h

    def _attach_to_event(self, event):
        pass

    def event(self, event):
        self._attach_to_event(event)

        edm_event = event.edm_event

        edm_event.getByLabel('hfprereco', self.handleHFPreRecHit)
        hfPreRecoHits = self.handleHFPreRecHit.product()

        # for i in range(hfPreRecoHits.size()):
        #     rechit =  hfPreRecoHits[i]
        #     print rechit.id()

        for rechit in hfPreRecoHits:
            print rechit.id().ieta(),
            print rechit.id().iphi(),
            print rechit.getHFQIE10Info(0).charge(),
            print rechit.getHFQIE10Info(0).energy(),
            print rechit.getHFQIE10Info(0).timeRising(),
            print rechit.getHFQIE10Info(0).timeFalling(),
            print rechit.getHFQIE10Info(0).nRaw(),
            print rechit.getHFQIE10Info(0).soi(),
            print

        # print hfPreRecoHit.getHFQIE10Info(0)
        # print hfPreRecoHit.getHFQIE10Info(1)

    def end(self):
        self.handleHFPreRecHit = None

##__________________________________________________________________||
from DataFormats.FWLite import Handle
# https://github.com/cms-sw/cmssw/blob/CMSSW_8_1_X/DataFormats/FWLite/python/__init__.py

##__________________________________________________________________||
