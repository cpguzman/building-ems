import pyomo
#import pyomo.opt
import pyomo.environ as pyo
import numpy as np
import pandas as pd
import matplotlib as plt
import math

# Function used to format the data in order to use with pyomo library.
def _auxDictionary(a):
    temp_dictionary = {}
    if len(a.shape) == 3:
        for dim0 in np.arange(a.shape[0]):
            for dim1 in np.arange(a.shape[1]):
                for dim2 in np.arange(a.shape[2]):
                    temp_dictionary[(dim0+1, dim1+1, dim2+1)] = a[dim0, dim1, dim2]
    elif len(a.shape) == 2:
        for dim0 in np.arange(a.shape[0]):
            for dim1 in np.arange(a.shape[1]):
                temp_dictionary[(dim0+1, dim1+1)] = a[dim0, dim1]
    else:
        for dim0 in np.arange(a.shape[0]):
            temp_dictionary[(dim0+1)] = a[dim0]
    return temp_dictionary

#**************************************Data definition******************************************
data = {}

# CSV files from which the information is being retrieved.
data['energy_price'] = pd.read_csv('energy_price.csv')
data['evs_inputs'] = pd.read_csv('evs_inputs.csv')
data['alpha'] = pd.read_csv('alpha.csv')
data['css_inputs'] = pd.read_csv('css_inputs.csv')
data['S'] = pd.read_csv('s.csv')
data['cp_inputs'] = pd.read_csv('cp_inputs.csv')
data['fases'] = pd.read_csv('fases.csv')
data['pl'] = pd.read_csv('pl.csv')
data['pt'] = pd.read_csv('pt.csv')
data['pv'] = pd.read_csv('pv.csv')
data['css_power'] = pd.read_csv('css_power.csv')
data['bess_inputs'] = pd.read_csv('bess_inputs.csv')

# Variables representing time, electric vehicles, charging points, and shared stations.
n_time = data['energy_price']['dT'].size
n_evs = data['evs_inputs']['Esoc'].size
cp = data['cp_inputs']['cs_id'].size
css = data['css_inputs']['cs_id'].size
fases = data['fases']['line'].size
n_bat = data['bess_inputs']['initial_soc'].size

print(f"\nEVs: {n_evs}\nCharging Station {css}\nCharging Points: {cp}\nPhases: {fases}\nBats: {n_bat}")

#***************************************Star time definition**********************************
from datetime import datetime
now = datetime.now()
start_time = now.strftime("%H:%M:%S")
print("Start Time =", start_time)


#***************************************Sets definition****************************************
model = pyo.ConcreteModel()
model.ev = pyo.Set(initialize = np.arange(1, n_evs + 1))
model.t = pyo.Set(initialize = np.arange(1, n_time + 1))
model.cs = pyo.Set(initialize = np.arange(1, css + 1))
model.cp = pyo.Set(initialize = np.arange(1, cp + 1))
model.f = pyo.Set(initialize = np.arange(1, fases + 1))
model.bat = pyo.Set(initialize = np.arange(1, n_bat + 1))

#***************************************Parameters definition************************************

# --- Production / Load Consumption ---
model.pt = pyo.Param(model.f, model.t, initialize =_auxDictionary(data['pt'].to_numpy()))       # phase power limit
model.pv = pyo.Param(model.f, model.t, initialize =_auxDictionary(data['pv'].to_numpy()))       # PV production
model.pl = pyo.Param(model.f, model.t, initialize =_auxDictionary(data['pl'].to_numpy()))       # Load consumption
model.dT = pyo.Param(model.t, initialize =_auxDictionary(data['energy_price'].to_numpy()[:,0])) # dT
model.import_price = pyo.Param(model.t, initialize =_auxDictionary(data['energy_price'].to_numpy()[:,1]))
model.export_price = pyo.Param(model.t, initialize =_auxDictionary(data['energy_price'].to_numpy()[:,2]))

# --- Connections ---
model.my_cs_id_cp = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,0])) # to which cs is the cp connected
model.cpconnected = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,8])) # to which cp is the EV connected
model.my_cp_fases = pyo.Param(model.cp, initialize=_auxDictionary(data['cp_inputs'].to_numpy()[:,8]))  # to which phase is the cp connected


# --- BESS ---
model.bess_max_charge_rate = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,1]))  
model.bess_max_discharge_rate = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,2])) 
model.EBessInitial = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,0])) 
model.bess_charge_efficiency = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,5])) 
model.bess_discharge_efficiency = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,5])) 
model.EBessMax = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,3]))  
model.EBessMin = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,4]))  


# --- EVs ---
model.ESoc = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,0]))   # Initial SoC
model.EEVmin = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,1])) # Minimum battery limit
model.EEVmax = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,2])) # Maximum battery capacity
model.target = pyo.Param(model.ev, initialize=_auxDictionary(data['evs_inputs'].to_numpy()[:,11])) # departue SoC target (%)
model.PchmaxEV = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,4])) # Max. EV charging power
model.PdchmaxEV = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,5]))# Max. EV discharging power
model.evcheff = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,6]))  # EV charging efficiency
model.evdcheff = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,7])) # EV discharging efficiency
model.v2gev = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,9]))    # V2G EV capability (0 or 1)
model.alpha = pyo.Param(model.ev, model.t, initialize = _auxDictionary(data['alpha'].to_numpy()))    # EV availability (connected to the charger)

# --- Station / Connector characteristics ---
model.Pcsmax = pyo.Param(model.f, model.cs, initialize = _auxDictionary(data['css_power'].to_numpy())) # Station power limit per phase
model.Pcpmax = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,3]))      # Max. Connector power
model.Pcpmin = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,5]))      # Min. Connector power
model.cheff = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,1]))       # Connector charging efficiency
model.dcheff = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,2]))      # Connector dischargig efficiency
model.type_ = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,9]))       # Continuous / discrete charger
model.v2gcp = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,4]))       # V2G connecor capability (0 or 1)

# --- Pen. / Factors ---
model.penalty1 = 1000       # Under limit battery penalty
model.penalty2 = 1000000    # Departue target missing penalty
model.pc_penalty = 10       # Surpassing contracted power penalty
model.DegCost =  0.000001   # Battery degradation cost (V2G)
model.m = 1e-7              # Soft Target


#***************************************Variables definition********************

# --- EVs ---
model.PEV = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0)   # Charging power
model.PEVdc = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0) # Discharging power
model.EEV = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0)   # Energy in the battery
model.a = pyo.Var(model.ev, model.t, domain = pyo.Binary, bounds=(0, 1), initialize=0)  # Charging (Binary)
model.b = pyo.Var(model.ev, model.t, domain = pyo.Binary, bounds=(0, 1), initialize=0)  # Discharging (Binary)

# --- BESS ---
model.bess_is_charging = pyo.Var(model.bat, model.t, domain = pyo.Binary, bounds=(0, 1), initialize = 0)
model.bess_is_discharging = pyo.Var(model.bat, model.t, domain = pyo.Binary, bounds=(0, 1), initialize = 0)
model.PBess = pyo.Var(model.bat, model.t, domain = pyo.NonNegativeReals, initialize = 0)  
model.PBessdc = pyo.Var(model.bat, model.t, domain = pyo.NonNegativeReals, initialize = 0) 
model.EBess = pyo.Var(model.bat, model.t, domain = pyo.NonNegativeReals, initialize = 0)  

# --- Connectors ---
model.PCP = pyo.Var(model.cp, model.t, domain = pyo.Reals, initialize = 0)              # Connector charging power
model.PCPdc = pyo.Var(model.cp, model.t, domain = pyo.NonNegativeReals, initialize = 0)            # Connector discharging power
model.cpa = pyo.Var(model.cp, model.t, domain = pyo.Binary, bounds=(0, 1), initialize=0)# Active connector (Binary)

# --- Grid ---

# monofasico
model.grid_import = pyo.Var(model.t, domain=pyo.NonNegativeReals, initialize = 0)
model.grid_export = pyo.Var(model.t, domain=pyo.NonNegativeReals, initialize = 0) 
'''
trifasico
model.grid_import = pyo.Var(model.f, model.t, domain=pyo.NonNegativeReals, initialize = 0)
model.grid_export = pyo.Var(model.f, model.t, domain=pyo.NonNegativeReals, initialize = 0) 
'''

model.is_importing = pyo.Var(model.t, domain=pyo.Binary, bounds=(0, 1), initialize=0)      # System importing? (Binary)
model.is_exporting = pyo.Var(model.t, domain=pyo.Binary, bounds=(0, 1), initialize=0)      # System exporting? (Binary)

# --- Relax. variables  ---
model.Eminsocrelax = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0) # lower battery limit relax
model.Etargetrelax = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0) # target relax
#monofasico
model.import_relax = pyo.Var(model.t, domain = pyo.NonNegativeReals, initialize = 0)
#trifasico: model.import_relax = pyo.Var(model.f, model.t, domain = pyo.NonNegativeReals, initialize = 0)  # Imported power relax



#****************************************************Connectors constraints******************************************************
# Power consumption of each Connectors related to each EV charging and discharging connected to its  

def _conn_power_consumption(m, ev, t, cp): 
    if cp == int(round(pyo.value(m.cpconnected[ev]))):
        return m.PCP[cp,t] == m.PEV[ev,t] - m.PEVdc[ev,t]
    return pyo.Constraint.Skip
model.conn_power_consumption = pyo.Constraint(model.ev, model.t, model.cp, rule = _conn_power_consumption)

# Max Charge
def _conn_power_charging_limit_max(m, ev, t, cp): 
    if cp == int(round(pyo.value(m.cpconnected[ev]))):
        if m.type_[cp] == 1:  # continuous
            return m.PCP[cp,t] <= m.Pcpmax[cp] * m.cpa[cp,t] * m.alpha[ev,t]
        else:                 # discrete
            return m.PCP[cp,t] == m.Pcpmax[cp] * m.cpa[cp,t] * m.alpha[ev,t]
    return pyo.Constraint.Skip
model.conn_power_charge_limit_max = pyo.Constraint(model.ev, model.t, model.cp, rule = _conn_power_charging_limit_max)

# Min Charge 
def _conn_power_charging_limit_min(m, ev, t, cp): 
    if cp == int(round(pyo.value(m.cpconnected[ev]))):
        if m.type_[cp] == 1:  # only to continuous charger
            return m.PEV[ev,t] >= m.Pcpmin[cp] * m.cpa[cp,t] * m.alpha[ev,t]
    return pyo.Constraint.Skip
model.conn_power_charge_limit_min = pyo.Constraint(model.ev, model.t, model.cp, rule = _conn_power_charging_limit_min)

# V2G 
def _conn_power_discharging_limit_2(m, ev, t, cp): 
    if cp == int(round(pyo.value(m.cpconnected[ev]))): 
        if m.type_[cp] == 1:
            return m.PCP[cp,t] >= -m.Pcpmax[cp] * m.v2gcp[cp] * m.alpha[ev,t]
    return pyo.Constraint.Skip      
model.conn_power_discharge_limit_2 = pyo.Constraint(model.ev, model.t, model.cp, rule = _conn_power_discharging_limit_2)

#  EQUILIBRADO
# =============================================================================
# LIMITE DE POTÊNCIA DA ESTAÇÃO / GARAGEM (Por Fase e Por Estação)
# Impede a garagem de ultrapassar os limites físicos (2.3 kW por fase)
# =============================================================================

# 1. Limite Máximo de Consumo (Carga)
def _limite_estacao_fase_max(m, f, cs, t):
    # Encontra todos os EVs ligados a esta estação (cs) e a esta fase (f)
    evs_nesta_fase = [
        ev for ev in m.ev
        if int(round(pyo.value(m.my_cs_id_cp[int(round(pyo.value(m.cpconnected[ev])))]))) == cs
        and int(round(pyo.value(m.my_cp_fases[int(round(pyo.value(m.cpconnected[ev])))]))) == f
    ]
    
    # Se não houver nenhum posto/carro ligado a esta fase, salta a restrição
    if not evs_nesta_fase:
        return pyo.Constraint.Skip
        
    # Soma a potência líquida e garante que não ultrapassa o máximo (ex: 2300W)
    potencia_fase = sum(m.PEV[ev, t] - m.PEVdc[ev, t] for ev in evs_nesta_fase)
    return potencia_fase <= m.Pcsmax[f, cs]

model.limite_estacao_fase_max = pyo.Constraint(model.f, model.cs, model.t, rule=_limite_estacao_fase_max)


# 2. Limite Máximo de Injeção / V2G (Descarga)
def _limite_estacao_fase_min(m, f, cs, t):
    # Encontra os mesmos EVs
    evs_nesta_fase = [
        ev for ev in m.ev
        if int(round(pyo.value(m.my_cs_id_cp[int(round(pyo.value(m.cpconnected[ev])))]))) == cs
        and int(round(pyo.value(m.my_cp_fases[int(round(pyo.value(m.cpconnected[ev])))]))) == f
    ]
    
    if not evs_nesta_fase:
        return pyo.Constraint.Skip
        
    # Garante que a injeção (potência negativa) não excede o limite da infraestrutura (ex: -2300W)
    potencia_fase = sum(m.PEV[ev, t] - m.PEVdc[ev, t] for ev in evs_nesta_fase)
    return potencia_fase >= -m.Pcsmax[f, cs]

model.limite_estacao_fase_min = pyo.Constraint(model.f, model.cs, model.t, rule=_limite_estacao_fase_min)


""" Desequilibrado
# =============================================================================
# LIMITE DE POTÊNCIA DA ESTAÇÃO / GARAGEM (TOTAL GLOBAL)
# Impede a garagem de ultrapassar a soma das 3 fases (ex: 2300 + 2300 + 2300 = 6900W) - Desequilibrado
# =============================================================================

# 1. Limite Máximo de Consumo Total (Carga)
def _limite_estacao_total_max(m, cs, t):
    # Encontra todos os EVs ligados a esta estação (cs), ignorando as fases
    evs_nesta_estacao = [
        ev for ev in m.ev
        if int(round(pyo.value(m.my_cs_id_cp[int(round(pyo.value(m.cpconnected[ev])))]))) == cs
    ]
    
    if not evs_nesta_estacao:
        return pyo.Constraint.Skip
        
    # Soma a potência líquida de todos os carros da estação
    potencia_estacao = sum(m.PEV[ev, t] - m.PEVdc[ev, t] for ev in evs_nesta_estacao)
    
    # O Limite passa a ser a soma do Pcsmax das 3 fases (ex: 2300 * 3 = 6900W)
    limite_global = sum(m.Pcsmax[f, cs] for f in m.f)
    
    return potencia_estacao <= limite_global

model.limite_estacao_total_max = pyo.Constraint(model.cs, model.t, rule=_limite_estacao_total_max)


# 2. Limite Máximo de Injeção Total / V2G (Descarga)
def _limite_estacao_total_min(m, cs, t):
    evs_nesta_estacao = [
        ev for ev in m.ev
        if int(round(pyo.value(m.my_cs_id_cp[int(round(pyo.value(m.cpconnected[ev])))]))) == cs
    ]
    
    if not evs_nesta_estacao:
        return pyo.Constraint.Skip
        
    potencia_estacao = sum(m.PEV[ev, t] - m.PEVdc[ev, t] for ev in evs_nesta_estacao)
    limite_global = sum(m.Pcsmax[f, cs] for f in m.f)
    
    return potencia_estacao >= -limite_global

model.limite_estacao_total_min = pyo.Constraint(model.cs, model.t, rule=_limite_estacao_total_min)
"""

# =============================================================================
# RESTRIÇÕES DOS VEÍCULOS ELÉTRICOS (EVs)
# =============================================================================

# Limite máximo de carga do EV
def _power_charging_limit(m,ev,t): 
    return m.PEV[ev,t] <= m.PchmaxEV[ev] * m.alpha[ev,t] * m.a[ev,t]
model.power_charging_limit2 = pyo.Constraint(model.ev, model.t, rule = _power_charging_limit)

# Limite máximo de descarga do EV (V2G)
def _power_discharging_limit(m,ev,t): 
    return m.PEVdc[ev,t] <= m.PdchmaxEV[ev] * m.alpha[ev,t] * m.b[ev,t] * m.v2gev[ev] 
model.power_discharging_limit2 = pyo.Constraint(model.ev, model.t, rule = _power_discharging_limit)

# Impede o carregamento e descarregamento em simultâneo
def _charging_discharging(m,ev,t): 
    return m.a[ev,t] + m.b[ev,t] <= 1 
model.charging_discharging = pyo.Constraint(model.ev, model.t, rule = _charging_discharging)

# Balanço de energia das baterias (SoC)
def _balance_energy_EVS(m,ev,t): 
    # Descobre a qual posto (cp) este EV está ligado para calcular a eficiência global
    cp_id = int(round(pyo.value(m.cpconnected[ev])))
    eff_ch = m.evcheff[ev] * m.cheff[cp_id]
    eff_dch = m.evdcheff[ev] * m.dcheff[cp_id]
    
    if t == 1:
        return m.EEV[ev,t] == m.ESoc[ev] + m.PEV[ev,t]*m.dT[t]*eff_ch - m.PEVdc[ev,t]*m.dT[t]/eff_dch
    else:
        return m.EEV[ev,t] == m.EEV[ev,t-1] + m.PEV[ev,t]*m.dT[t]*eff_ch - m.PEVdc[ev,t]*m.dT[t]/eff_dch
model.balance_energy_EVS = pyo.Constraint(model.ev, model.t, rule = _balance_energy_EVS)

# Limite mínimo de segurança da bateria (com relaxamento)
def _energy_limits_EVS_1(m,ev,t): 
    return m.EEV[ev,t] + m.Eminsocrelax[ev,t] >= m.EEVmin[ev]
model.energy_limits_EVS_1 = pyo.Constraint(model.ev, model.t, rule = _energy_limits_EVS_1)

# Limite físico máximo da bateria
def _energy_limits_EVS_2(m,ev,t): 
    return m.EEV[ev,t] <= m.EEVmax[ev] 
model.energy_limits_EVS_2 = pyo.Constraint(model.ev, model.t, rule = _energy_limits_EVS_2)  

# Target de carga para a hora de partida (último instante de tempo)
def _balance_energy_EVS3(m,ev,t): 
    if t == m.t.last(): 
        return m.EEV[ev,t] + m.Etargetrelax[ev,t] >= m.EEVmax[ev] * m.target[ev] 
    return pyo.Constraint.Skip
model.balance_energy_EVS3 = pyo.Constraint(model.ev, model.t, rule = _balance_energy_EVS3)


# =============================================================================
# BESS constraints
# =============================================================================

# BESS limite máximo de carga (em W)
def _max_charge_BESS(m, b, t):
    return m.PBess[b, t] <= m.bess_max_charge_rate[b] * m.bess_is_charging[b, t]
model.max_charge_BESS = pyo.Constraint(model.bat, model.t, rule=_max_charge_BESS)

# BESS limite máximo de descarga (em W)
def _max_discharge_BESS(m, b, t):
    return m.PBessdc[b, t] <= m.bess_max_discharge_rate[b] * m.bess_is_discharging[b, t]
model.max_discharge_BESS = pyo.Constraint(model.bat, model.t, rule=_max_discharge_BESS)

# Impede o carregamento e descarregamento em simultâneo
def _bess_status(m, b, t):
    return m.bess_is_charging[b, t] + m.bess_is_discharging[b, t] <= 1
model.bess_status = pyo.Constraint(model.bat, model.t, rule=_bess_status)

# Balanço de energia da bateria em Wh
def _bess_energy_balance(m, b, t):
    if t == m.t.first(): # t == 1
        return m.EBess[b, t] == m.EBessInitial[b]*m.EBessMax[b] + m.PBess[b, t]*m.dT[t]*m.bess_charge_efficiency[b] - m.PBessdc[b, t]*m.dT[t]/m.bess_discharge_efficiency[b]
    else:
        return m.EBess[b, t] == m.EBess[b, t - 1] + m.PBess[b, t]*m.dT[t]*m.bess_charge_efficiency[b] - m.PBessdc[b, t]*m.dT[t]/m.bess_discharge_efficiency[b]
model.bess_energy_balance = pyo.Constraint(model.bat, model.t, rule=_bess_energy_balance)

# BESS capacidade física máxima em Wh
def _bess_max_capacity(m, b, t):
    return m.EBess[b, t] <= m.EBessMax[b]
model.bess_max_capacity = pyo.Constraint(model.bat, model.t, rule=_bess_max_capacity)

# BESS capacidade física mínima em Wh
def _bess_min_energy(m, b, t):
    return m.EBess[b, t] >= m.EBessMin[b]
model.bess_min_energy = pyo.Constraint(model.bat, model.t, rule=_bess_min_energy)

# BESS capacidade alvo no final do período (neste caso, deixar a bateria a 40%)
def _bess_target_energy(m, b, t):
    if t == m.t.last():
        return m.EBess[b, t] >= m.EBessMax[b] * 0.4
    return pyo.Constraint.Skip
model.bess_target_energy = pyo.Constraint(model.bat, model.t, rule=_bess_target_energy)


#Energy balance in the system considering threephase balanced PV, threephase unbalanced load consumption, and threphase unbalanced CS 
#def _energy_balance(m,f,t): 
#    #return m.grid_import[t]  == sum(m.PCS[cs,t] for cs in m.cs)
#    return m.grid_import[f,t] - model.grid_export[f,t]  == sum([m.PEV[ev,t] - m.PEVdc[ev,t] for ev in m.ev]) + m.pl[f,t] - m.pv[f,t] 
#model._energy_balance = pyo.Constraint(model.f, model.t, rule =_energy_balance)  

# =============================================================================
# BALANÇO GERAL DE ENERGIA E LIMITES DA REDE
# =============================================================================

def _energy_balance(m, t): 
    ev_net_power = sum(m.PEV[ev,t] - m.PEVdc[ev,t] for ev in m.ev)
    
    bess_net_power = sum(m.PBess[b, t] - m.PBessdc[b, t] for b in m.bat)
    #bess_net_power = 0 #PV + EV
    
    total_pl = sum(m.pl[f,t] for f in m.f)
    total_pv = sum(m.pv[f,t] for f in m.f)
    
    # Balanço Final 
    return m.grid_import[t] - m.grid_export[t] == ev_net_power + bess_net_power + total_pl - total_pv 

model.energy_balance = pyo.Constraint(model.t, rule =_energy_balance)

# Limite máximo de Importação Global
def _contracted_power_constraint(m, t): 
    total_pt = sum(m.pt[f,t] for f in m.f) # Soma a potência contratada das 3 fases
    return m.grid_import[t] <= total_pt * m.is_importing[t] + m.import_relax[t]
model.contracted_power_constraint = pyo.Constraint(model.t, rule =_contracted_power_constraint) 

# Limite máximo de Exportação Global
def _contracted_power_constraint2(m, t): 
    total_pt = sum(m.pt[f,t] for f in m.f)
    return m.grid_export[t] <= total_pt * m.is_exporting[t] 
model.contracted_power_constraint2 = pyo.Constraint(model.t, rule =_contracted_power_constraint2)
""" trifasico
def _energy_balance(m, f, t): 
    # Mapeia apenas os EVs que estão ligados à fase 'f'
    evs_nesta_fase = [
        ev for ev in m.ev
        if int(round(pyo.value(m.my_cp_fases[int(round(pyo.value(m.cpconnected[ev])))]))) == f
    ]
    
    # Soma o consumo líquido (carga - descarga) apenas dos EVs ligados a esta fase
    if evs_nesta_fase:
        ev_net_power = sum(m.PEV[ev,t] - m.PEVdc[ev,t] for ev in evs_nesta_fase)
    else:
        ev_net_power = 0
        
    # Soma o consumo líquido do BESS (carga - descarga) iterando sobre todas as baterias.
    # Como assumimos que é trifásico e perfeitamente equilibrado, dividimos por 3.
    bess_net_power = sum(m.PBess[b, t] - m.PBessdc[b, t] for b in m.bat) / 3
        
    # Balanço Final: 
    # Importação - Exportação = Consumo EVs + Consumo BESS + Cargas Instalação (pl) - Produção Solar (pv)
    return m.grid_import[f,t] - m.grid_export[f,t] == ev_net_power + bess_net_power + m.pl[f,t] - m.pv[f,t] 

model.energy_balance = pyo.Constraint(model.f, model.t, rule =_energy_balance)

# Limite máximo de Importação 
def _contracted_power_constraint(m, f, t): 
    # O import_relax permite ao solver pagar a pc_penalty em vez de dar erro "Infeasible"
    return m.grid_import[f,t] <= m.pt[f,t] * m.is_importing[t] + m.import_relax[f,t]
model.contracted_power_constraint = pyo.Constraint(model.f, model.t, rule =_contracted_power_constraint) 

# Limite máximo de Exportação
def _contracted_power_constraint2(m, f, t): 
    return m.grid_export[f,t] <= m.pt[f,t] * m.is_exporting[t] 
model.contracted_power_constraint2 = pyo.Constraint(model.f, model.t, rule =_contracted_power_constraint2)  
"""
# Impede a Importação e Exportação em simultâneo
def _importing_exporting(m, t): 
    return m.is_importing[t] + m.is_exporting[t] <= 1
model.importing_exporting = pyo.Constraint(model.t, rule =_importing_exporting)


#************************************************************************Objective Function***********************************************************
#monofasico
def _FOag(m):
    # 1. Custos da Rede
    custos_rede = sum(
        (m.grid_import[t] * m.dT[t] * m.import_price[t]) 
        - (m.grid_export[t] * m.dT[t] * m.export_price[t]) 
        + (m.import_relax[t] * m.dT[t] * m.pc_penalty)
        for t in np.arange(1, n_time + 1)
    )
    
# 2. Custos e Penalizações dos EVs
    custos_evs = sum(
        # Custo Físico: Energia descarregada (kWh) * Custo Fixo de Degradação (€/kWh)
        (m.PEVdc[ev,t] * m.dT[t] * m.DegCost)
        
        # Penalizações de Relaxamento (Ativadas apenas quando as restrições falham)
        + (m.Eminsocrelax[ev,t] * m.penalty1)
        + (m.Etargetrelax[ev,t] * m.penalty2)
        
        for ev in np.arange(1, n_evs + 1) 
        for t in np.arange(1, n_time + 1)
    )
    return custos_rede + custos_evs
"""trifasico
def _FOag(m):
    # 1. Custos da Rede
    custos_rede = sum(
        (m.grid_import[f,t] * m.dT[t] * m.import_price[t]) 
        - (m.grid_export[f,t] * m.dT[t] * m.export_price[t]) 
        + (m.import_relax[f,t] * m.dT[t] * m.pc_penalty)
        for f in np.arange(1, fases + 1) 
        for t in np.arange(1, n_time + 1)
    )
    
    # 2. Custos e Penalizações dos EVs
    custos_evs = sum(
        # Custo de degradação da bateria 
        (m.PEVdc[ev,t] * m.dT[t] * m.export_price[t] * m.DegCost)
        
        # "empurrão" matemático que usa o m (1e-7) e o target 
        + ((m.EEVmax[ev] * m.target[ev]) - m.EEV[ev,t]) * m.m
        
        # Penalização por bateria muito baixa
        + (m.Eminsocrelax[ev,t] * m.penalty1)
        
        # Penalização por falhar a energia target
        + (m.Etargetrelax[ev,t] * m.penalty2)
        
        for ev in np.arange(1, n_evs + 1) 
        for t in np.arange(1, n_time + 1)
    )
    
    return custos_rede + custos_evs
"""
model.FOag = pyo.Objective(rule = _FOag, sense = pyo.minimize)

#************************************************************************Solve the model***********************************************************
from pyomo.opt import SolverFactory
model.write('res_V4_EC.lp',  io_options={'symbolic_solver_labels': True})

opt = pyo.SolverFactory('cplex', executable='C:\\Program Files\\IBM\\ILOG\\CPLEX_Studio2211\\cplex\\bin\\x64_win64\\cplex.exe')
opt.options['LogFile'] = 'res_V4_EC.log'

results = opt.solve(model)#, tee=True)
results.write()

#************************************************************************End Time information***********************************************************
print("\nFunção Objetivo =", pyo.value(model.FOag))

now = datetime.now()
end_time = now.strftime("%H:%M:%S")
print("End Time =", end_time)
print("Dif: {}".format(datetime.strptime(end_time, "%H:%M:%S") - datetime.strptime(start_time, "%H:%M:%S")))


def ext_pyomo_vals(vals):
    # Create a pandas Series from the Pyomo values
    s = pd.Series(vals.extract_values(),
                  index=vals.extract_values().keys())
    # Check if the Series is multi-indexed, if so, unstack it
    if type(s.index[0]) == tuple:    # it is multi-indexed
        s = s.unstack(level=1)
    else:
        # Convert Series to DataFrame
        s = pd.DataFrame(s)
    return s


# Converting Pyomo variables into DataFrames
PEV_df = ext_pyomo_vals(model.PEV)
PEVdc_df = ext_pyomo_vals(model.PEVdc)
PCP_df = ext_pyomo_vals(model.PCP)
PCPdc_df = ext_pyomo_vals(model.PCPdc)
dT_df = ext_pyomo_vals(model.dT)
import_price_df = ext_pyomo_vals(model.import_price)
export_price_df = ext_pyomo_vals(model.export_price)
EEV_df = ext_pyomo_vals(model.EEV)
grid_import_df = ext_pyomo_vals(model.grid_import) 
grid_export_df = ext_pyomo_vals(model.grid_export)

PBess_df = ext_pyomo_vals(model.PBess)
PBessdc_df = ext_pyomo_vals(model.PBessdc)
EBess_df = ext_pyomo_vals(model.EBess)
bess_charging_df = ext_pyomo_vals(model.bess_is_charging)
bess_discharging_df = ext_pyomo_vals(model.bess_is_discharging)

# Extracting three-phase data and organizing into separate columns (DINÂMICO)
#grid_import_df_3colums = pd.DataFrame(grid_import_df.values.T, index=range(1, n_time + 1))
#grid_import_df_3colums.columns = ['grid_import_ph1', 'grid_import_ph2', 'grid_import_ph3']   trifasico

#grid_export_df_3colums = pd.DataFrame(grid_export_df.values.T, index=range(1, n_time + 1))
#grid_export_df_3colums.columns = ['grid_export_ph1', 'grid_export_ph2', 'grid_export_ph3']

EEVmax_df = ext_pyomo_vals(model.EEVmax)
Etargetrelax_df = ext_pyomo_vals(model.Etargetrelax) # Atualizado
Eminsocrelax_df = ext_pyomo_vals(model.Eminsocrelax)


charge_cost = sum([PEV_df[t][ev]*dT_df[0][t]*import_price_df[0][t]
                   for ev in np.arange(1, n_evs + 1) for t in np.arange(1, n_time + 1)])

discharge_cost = sum([PEVdc_df[t][ev]*dT_df[0][t]*import_price_df[0][t]
                      for ev in np.arange(1, n_evs + 1) for t in np.arange(1, n_time + 1)])

print('Charge cost: {}'.format(charge_cost))
print('Discharge cost: {}'.format(discharge_cost))

print("Total Charge: {}".format(np.sum(PEV_df.to_numpy())))
print("Total Discharge: {}".format(np.sum(PEVdc_df.to_numpy())))


import os 
folder = 'RESULTS_' + str(n_evs)

if not os.path.exists(folder):
    os.makedirs(folder)
    
# Guardar as variáveis
EEV_df.to_csv(folder + '/EEV.csv')
EEVmax_df.to_csv(folder + '/EEVmax.csv')
PEV_df.to_csv(folder + '/PEV.csv')
PCP_df.to_csv(folder + '/PCP.csv')
grid_import_df.to_csv(folder + '/grid_import.csv')
grid_export_df.to_csv(folder + '/grid_export.csv')
import_price_df.to_csv(folder + '/import_price.csv')
export_price_df.to_csv(folder + '/export_price.csv')
# Guardar as variáveis da rede (agora globais/monofásicas)


PEVdc_df.to_csv(folder + '/PEVdc.csv')
PCPdc_df.to_csv(folder + '/PCPdc.csv')
PEV_df.sum().to_csv(folder + '/PEV_h.csv')
PEVdc_df.sum().to_csv(folder + '/PEVdc_h.csv')
grid_import_df.sum().to_csv(folder + '/grid_import_h.csv')
#grid_import_df_3colums.to_csv(folder + '/grid_import_per_phase.csv')

grid_export_df.sum().to_csv(folder + '/grid_export_h.csv')
#grid_export_df_3colums.to_csv(folder + '/grid_export_per_phase.csv')

Etargetrelax_df.to_csv(folder + '/Etargetrelax.csv')
Etargetrelax_df.sum().to_csv(folder + '/Etargetrelax_h.csv')

Eminsocrelax_df.to_csv(folder + '/Eminsocrelax.csv')
Eminsocrelax_df.sum().to_csv(folder + '/Eminsocrelax_h.csv')

PBess_df.to_csv(folder + '/PBess.csv')
PBessdc_df.to_csv(folder + '/PBessdc.csv')
EBess_df.to_csv(folder + '/EBess.csv')
bess_charging_df.to_csv(folder + '/bess_is_charging.csv')
bess_discharging_df.to_csv(folder + '/bess_is_discharging.csv')

# Totais horários (Somas) do BESS
PBess_df.sum().to_csv(folder + '/PBess_h.csv')
PBessdc_df.sum().to_csv(folder + '/PBessdc_h.csv')

# Creating a CSV with the grid accounts
#grid_accounts = pd.concat([import_price_df, export_price_df, grid_import_df_3colums, grid_export_df_3colums], axis=1)

# Renaming the columns
#novos_nomes = list(grid_accounts.columns)
#novos_nomes[0] = 'import_price'
#novos_nomes[1] = 'export_price'
#grid_accounts.columns = novos_nomes

# Doing the accounts
#grid_accounts['grid_import_ph1*import_price'] = grid_accounts['grid_import_ph1'] * grid_accounts['import_price']
#grid_accounts['grid_import_ph2*import_price'] = grid_accounts['grid_import_ph2'] * grid_accounts['import_price']
#grid_accounts['grid_import_ph3*import_price'] = grid_accounts['grid_import_ph3'] * grid_accounts['import_price']
#grid_accounts['total_grid_import*import_price'] = grid_accounts['grid_import_ph1*import_price'] + grid_accounts['grid_import_ph2*import_price'] + grid_accounts['grid_import_ph3*import_price']

#grid_accounts['grid_export_ph1*export_price'] = grid_accounts['grid_export_ph1'] * grid_accounts['export_price']
#grid_accounts['grid_export_ph2*export_price'] = grid_accounts['grid_export_ph2'] * grid_accounts['export_price']
#grid_accounts['grid_export_ph3*export_price'] = grid_accounts['grid_export_ph3'] * grid_accounts['export_price']
#grid_accounts['total_grid_export*export_price'] = grid_accounts['grid_export_ph1*export_price'] + grid_accounts['grid_export_ph2*export_price'] + grid_accounts['grid_export_ph3*export_price']

#grid_accounts.to_csv(folder + '/grid_accounts.csv')

# Penalty calculations
target_penalty = Etargetrelax_df * model.penalty2
target_penalty.to_csv(folder + '/target_times_penalty.csv')


# Creating a DataFrame to calculate EV accounts
EEVmax_values = EEVmax_df.values.tolist()
EEVmax_values = [value for sublist in EEVmax_values for value in sublist]

ev_accounts = pd.DataFrame([[(EEVmax_values[i] - EEV_df.iloc[i, j]) * 0.1 for j in range(len(EEV_df.columns))] for i in range(len(EEVmax_values))], columns=EEV_df.columns)
ev_accounts.to_csv(folder + '/ev_accounts.csv')


# =============================================================================
# EXTRACÇÃO DOS DADOS PARA EXCEL (Fluxos de Potência + Preços + Estado dos EVs)
# =============================================================================

# 1. Extrair Variáveis de Potência
pl_df = ext_pyomo_vals(model.pl)
pv_df = ext_pyomo_vals(model.pv)

# 2. Extrair Variáveis de Estado (Disponibilidade e Ação dos EVs)
alpha_df = ext_pyomo_vals(model.alpha) 
a_df = ext_pyomo_vals(model.a)         
b_df = ext_pyomo_vals(model.b)         

# 3. Agregações (Somar fases e EVs)
if len(pl_df.index) == fases:
    total_pl = pl_df.sum(axis=0)
    total_pv = pv_df.sum(axis=0)
else:
    total_pl = pl_df.sum(axis=1)
    total_pv = pv_df.sum(axis=1)

if len(PEV_df.index) == n_time:
    total_EV_charge = PEV_df.sum(axis=1)
    total_EV_discharge = PEVdc_df.sum(axis=1)
    total_evs_connected = alpha_df.sum(axis=1)
    total_evs_charging = a_df.sum(axis=1)
    total_evs_discharging = b_df.sum(axis=1)
else:
    total_EV_charge = PEV_df.sum(axis=0)
    total_EV_discharge = PEVdc_df.sum(axis=0)
    total_evs_connected = alpha_df.sum(axis=0)
    total_evs_charging = a_df.sum(axis=0)
    total_evs_discharging = b_df.sum(axis=0)
    
if len(PBess_df.index) == n_time:
    total_BESS_charge = PBess_df.sum(axis=1)
    total_BESS_discharge = PBessdc_df.sum(axis=1)
    total_BESS_soc = EBess_df.sum(axis=1)
else:
    total_BESS_charge = PBess_df.sum(axis=0)
    total_BESS_discharge = PBessdc_df.sum(axis=0)
    total_BESS_soc = EBess_df.sum(axis=0)

#total_grid_import = grid_import_df_3colums.sum(axis=1)    trifasico
#total_grid_export = grid_export_df_3colums.sum(axis=1)

total_grid_import = grid_import_df.sum(axis=1)    #monofasico
total_grid_export = grid_export_df.sum(axis=1)

# Extrair Preços
preco_imp = import_price_df.values.flatten()
preco_exp = export_price_df.values.flatten()

# 4. Criar a Tabela Final
fluxos_df = pd.DataFrame({
    'Instante': range(1, n_time + 1),
    'Preco_Importacao': preco_imp,
    'Preco_Exportacao': preco_exp,
    'VEs_Ligados_Total': total_evs_connected.values,
    'VEs_a_Carregar': total_evs_charging.values,
    'VEs_a_Descarregar': total_evs_discharging.values,
    'Rede_Importacao_kW': total_grid_import.values,
    'Rede_Exportacao_kW': total_grid_export.values,
    'VE_Carga_kW': total_EV_charge.values,
    'VE_Descarga_kW': total_EV_discharge.values,
    'BESS_Carga_kW': total_BESS_charge.values,       
    'BESS_Descarga_kW': total_BESS_discharge.values, 
    'Instalacao_PL_kW': total_pl.values,
    'Solar_PV_kW': total_pv.values
})
fluxos_df.set_index('Instante', inplace=True)

# =============================================================================
# EXTRACÇÃO DE ENERGIAS E RELAXAMENTOS PARA EXCEL
# =============================================================================

if len(EEV_df.index) == n_time:
    total_soc_kwh = EEV_df.sum(axis=1) 
    total_relax_minsoc = Eminsocrelax_df.sum(axis=1)
    total_relax_target = Etargetrelax_df.sum(axis=1)
else:
    total_soc_kwh = EEV_df.sum(axis=0)
    total_relax_minsoc = Eminsocrelax_df.sum(axis=0)
    total_relax_target = Etargetrelax_df.sum(axis=0)

# Criar uma coluna que soma todos os erros/penalizações
total_erros_relaxamento = total_relax_minsoc + total_relax_target

fluxos_df['Energia_Armazenada_VEs_kWh'] = total_soc_kwh.values
fluxos_df['Energia_Armazenada_BESS_kWh'] = total_BESS_soc.values
fluxos_df['Falhas_Relaxamento_kWh'] = total_erros_relaxamento.values


fluxos_df.to_excel(folder + '/Gestao_Energia_e_Veiculos_Final.xlsx', engine='openpyxl')

print("\nFicheiro gerado com Sucesso!")