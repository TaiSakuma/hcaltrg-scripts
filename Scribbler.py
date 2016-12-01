# Tai Sakuma <sakuma@cern.ch>
import os
import pandas as pd
import numpy as np

##__________________________________________________________________||
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 4096)
pd.set_option('display.max_rows', 65536)
pd.set_option('display.width', 1000)

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
        self.QIE10MergedDepth_eta_depth1 = [ ]
        self.QIE10MergedDepth_eta_depth2 = [ ]
        self.QIE10MergedDepth_phi_depth1 = [ ]
        self.QIE10MergedDepth_phi_depth2 = [ ]
        self._attach_to_event(event)

    def _attach_to_event(self, event):
        event.QIE10MergedDepth_ieta = self.QIE10MergedDepth_ieta
        event.QIE10MergedDepth_iphi = self.QIE10MergedDepth_iphi
        event.QIE10MergedDepth_index = self.QIE10MergedDepth_index
        event.QIE10MergedDepth_energy_depth1 = self.QIE10MergedDepth_energy_depth1
        event.QIE10MergedDepth_energy_depth2 = self.QIE10MergedDepth_energy_depth2
        event.QIE10MergedDepth_energy_ratio = self.QIE10MergedDepth_energy_ratio
        event.QIE10MergedDepth_eta_depth1 = self.QIE10MergedDepth_eta_depth1
        event.QIE10MergedDepth_eta_depth2 = self.QIE10MergedDepth_eta_depth2
        event.QIE10MergedDepth_phi_depth1 = self.QIE10MergedDepth_phi_depth1
        event.QIE10MergedDepth_phi_depth2 = self.QIE10MergedDepth_phi_depth2

    def event(self, event):
        self._attach_to_event(event)

        df = pd.DataFrame({
            'ieta': event.hfrechit_ieta,
            'iphi': event.hfrechit_iphi,
            'QIE10_index': event.hfrechit_QIE10_index,
            'depth': event.hfrechit_depth,
            'energy': event.hfrechit_QIE10_energy_th,
            'eta': event.hfrechit_eta,
            'phi': event.hfrechit_phi
        })

        df = pd.pivot_table(
            df,
            values = ['energy', 'eta', 'phi'],
            index = ['ieta', 'iphi', 'QIE10_index'],
            columns = ['depth']).reset_index()
        df.columns = ['_'.join(['{}'.format(f) for f in e]).strip('_') for e in df]
        df['energy_ratio'] = np.where(df.energy_2 > 0, df.energy_1/df.energy_2, 0)

        self.QIE10MergedDepth_ieta[:] = df.ieta
        self.QIE10MergedDepth_iphi[:] = df.iphi
        self.QIE10MergedDepth_index[:] = df.QIE10_index
        self.QIE10MergedDepth_energy_depth1[:] = df.energy_1
        self.QIE10MergedDepth_energy_depth2[:] =  df.energy_2
        self.QIE10MergedDepth_energy_ratio[:] = df.energy_ratio
        self.QIE10MergedDepth_eta_depth1[:] = df.eta_1
        self.QIE10MergedDepth_eta_depth2[:] = df.eta_2
        self.QIE10MergedDepth_phi_depth1[:] = df.phi_1
        self.QIE10MergedDepth_phi_depth2[:] = df.phi_2

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
class GenMatching(object):
    def begin(self, event):
        self.GenMatchedSummed_gen_index = [ ]
        self.GenMatchedSummed_qie_index = [ ]
        self.GenMatchedSummed_energy_depth1 = [ ]
        self.GenMatchedSummed_energy_depth2 = [ ]
        self.GenMatchedSummed_energy_ratio = [ ]

        self._attach_to_event(event)

    def _attach_to_event(self, event):
        event.GenMatchedSummed_gen_index = self.GenMatchedSummed_gen_index
        event.GenMatchedSummed_qie_index = self.GenMatchedSummed_qie_index
        event.GenMatchedSummed_energy_depth1 = self.GenMatchedSummed_energy_depth1
        event.GenMatchedSummed_energy_depth2 = self.GenMatchedSummed_energy_depth2
        event.GenMatchedSummed_energy_ratio = self.GenMatchedSummed_energy_ratio

    def event(self, event):
        self._attach_to_event(event)

        df_gen = pd.DataFrame(dict(
            gen_eta = event.genParticle_eta,
            gen_phi = event.genParticle_phi
        ))
        df_gen['gen_index'] = df_gen.index

        df_qie = pd.DataFrame(dict(
            qie_index = event.QIE10MergedDepth_index,
            energy_depth1 = event.QIE10MergedDepth_energy_depth1,
            energy_depth2 = event.QIE10MergedDepth_energy_depth2,
            eta_depth1 = event.QIE10MergedDepth_eta_depth1,
            eta_depth2 = event.QIE10MergedDepth_eta_depth2,
            phi_depth1 = event.QIE10MergedDepth_phi_depth1,
            phi_depth2 = event.QIE10MergedDepth_phi_depth2
        ))
        df_qie = df_qie[(df_qie.energy_depth1 > 0) & (df_qie.energy_depth2 > 0)]

        df_gen['dummy'] = 1
        df_qie['dummy'] = 1
        df = pd.merge(df_gen, df_qie, on = ['dummy'])
        del df['dummy']

        df['deta1'] = np.abs(df['gen_eta'] - df['eta_depth1'])
        df['dphi1'] = np.arccos(np.cos(df['gen_phi'] - df['phi_depth1']))
        df['deta2'] = np.abs(df['gen_eta'] - df['eta_depth2'])
        df['dphi2'] = np.arccos(np.cos(df['gen_phi'] - df['phi_depth2']))
        df['dr1'] = np.sqrt(df['deta1']**2 + df['dphi1']**2)
        df['dr2'] = np.sqrt(df['deta2']**2 + df['dphi2']**2)

        maxdr = 0.2
        df['matched'] = (df['dr1'] <= maxdr) & (df['dr2'] <= maxdr)

        df_matched = df[df['matched']]


        df_summed = df_matched.groupby(['gen_index', 'qie_index'])['energy_depth1', 'energy_depth2'].sum().reset_index()

        df_summed['energy_ratio'] = df_summed['energy_depth1']/df_summed['energy_depth2']

        self.GenMatchedSummed_gen_index[:] = df_summed.gen_index
        self.GenMatchedSummed_qie_index[:] = df_summed.qie_index
        self.GenMatchedSummed_energy_depth1[:] = df_summed.energy_depth1
        self.GenMatchedSummed_energy_depth2[:] = df_summed.energy_depth2
        self.GenMatchedSummed_energy_ratio[:] = df_summed.energy_ratio

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
