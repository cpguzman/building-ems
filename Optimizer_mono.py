import pyomo.environ as pyo
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import math
from datetime import datetime
import os 

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
data['cp_inputs'] = pd.read_csv('cp_inputs.csv')
data['pl'] = pd.read_csv('pl.csv')  # DEVE TER APENAS 1 COLUNA (Global)
data['pt'] = pd.read_csv('pt.csv')  # DEVE TER APENAS 1 COLUNA (Global)
data['pv'] = pd.read_csv('pv.csv')  # DEVE TER APENAS 1 COLUNA (Global)
data['css_power'] = pd.read_csv('css_power.csv') # DEVE TER APENAS 1 VALOR (Limite global)
data['bess_inputs'] = pd.read_csv('bess_inputs.csv')

# Variables representing time, electric vehicles, charging points, and shared stations.
n_time = data['energy_price']['dT'].size
n_evs = data['evs_inputs']['Esoc'].size
cp = data['cp_inputs']['cs_id'].size
css = data['css_inputs']['cs_id'].size
n_bat = data['bess_inputs']['initial_soc'].size

print(f"\nEVs: {n_evs}\nCharging Station {css}\nCharging Points: {cp}\nBats: {n_bat}")

#***************************************Star time definition**********************************
now = datetime.now()
start_time = now.strftime("%H:%M:%S")
print("Start Time =", start_time)

#***************************************Sets definition****************************************
model = pyo.ConcreteModel()
model.ev = pyo.Set(initialize = np.arange(1, n_evs + 1))
model.t = pyo.Set(initialize = np.arange(1, n_time + 1))
model.cs = pyo.Set(initialize = np.arange(1, css + 1))
model.cp = pyo.Set(initialize = np.arange(1, cp + 1))
model.bat = pyo.Set(initialize = np.arange(1, n_bat + 1))

#***************************************Parameters definition************************************

# --- Production / Load Consumption (Monofásico - Indexado apenas ao tempo) ---
model.pt = pyo.Param(model.t, initialize =_auxDictionary(data['pt'].to_numpy().flatten()))       
model.pv = pyo.Param(model.t, initialize =_auxDictionary(data['pv'].to_numpy().flatten()))       
model.pl = pyo.Param(model.t, initialize =_auxDictionary(data['pl'].to_numpy().flatten()))      
model.dT = pyo.Param(model.t, initialize =_auxDictionary(data['energy_price'].to_numpy()[:,0])) 
model.import_price = pyo.Param(model.t, initialize =_auxDictionary(data['energy_price'].to_numpy()[:,1]))
model.export_price = pyo.Param(model.t, initialize =_auxDictionary(data['energy_price'].to_numpy()[:,2]))

# --- Connections ---
model.my_cs_id_cp = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,0])) 
model.cpconnected = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,8])) 
# (my_cp_fases removido por já não ser necessário em monofásico)

# --- BESS ---
model.bess_max_charge_rate = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,1]))  
model.bess_max_discharge_rate = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,2])) 
model.EBessInitial = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,0])) 
model.bess_charge_efficiency = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,5])) 
model.bess_discharge_efficiency = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,5])) 
model.EBessMax = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,3]))  
model.EBessMin = pyo.Param(model.bat, initialize=_auxDictionary(data['bess_inputs'].to_numpy()[:,4]))  

# --- EVs ---
model.ESoc = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,0]))   
model.EEVmin = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,1])) 
model.EEVmax = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,2])) 
model.target = pyo.Param(model.ev, initialize=_auxDictionary(data['evs_inputs'].to_numpy()[:,11])) 
model.PchmaxEV = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,4])) 
model.PdchmaxEV = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,5]))
model.evcheff = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,6]))  
model.evdcheff = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,7])) 
model.v2gev = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,9]))    
model.alpha = pyo.Param(model.ev, model.t, initialize = _auxDictionary(data['alpha'].to_numpy()))    

# --- Station / Connector characteristics ---
model.Pcsmax = pyo.Param(model.cs, initialize = _auxDictionary(data['css_power'].to_numpy().flatten()))
model.Pcpmax = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,3]))      
model.Pcpmin = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,5]))      
model.cheff = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,1]))       
model.dcheff = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,2]))      
model.type_ = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,9]))       
model.v2gcp = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,4]))       

# --- Pen. / Factors ---
model.penalty1 = 1000       
model.penalty2 = 1000000    
model.pc_penalty = 10       
model.DegCost =  0.000001   
model.m = 1e-7              


#***************************************Variables definition********************

# --- EVs ---
model.PEV = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0)   
model.PEVdc = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0) 
model.EEV = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0)   
model.a = pyo.Var(model.ev, model.t, domain = pyo.Binary, bounds=(0, 1), initialize=0)  
model.b = pyo.Var(model.ev, model.t, domain = pyo.Binary, bounds=(0, 1), initialize=0)  

# --- BESS ---
model.bess_is_charging = pyo.Var(model.bat, model.t, domain = pyo.Binary, bounds=(0, 1), initialize = 0)
model.bess_is_discharging = pyo.Var(model.bat, model.t, domain = pyo.Binary, bounds=(0, 1), initialize = 0)
model.PBess = pyo.Var(model.bat, model.t, domain = pyo.NonNegativeReals, initialize = 0)  
model.PBessdc = pyo.Var(model.bat, model.t, domain = pyo.NonNegativeReals, initialize = 0) 
model.EBess = pyo.Var(model.bat, model.t, domain = pyo.NonNegativeReals, initialize = 0)  

# --- Connectors ---
model.PCP = pyo.Var(model.cp, model.t, domain = pyo.Reals, initialize = 0)              
model.PCPdc = pyo.Var(model.cp, model.t, domain = pyo.NonNegativeReals, initialize = 0) 
model.cpa = pyo.Var(model.cp, model.t, domain = pyo.Binary, bounds=(0, 1), initialize=0)

# --- Grid (Monofásico) ---
model.grid_import = pyo.Var(model.t, domain=pyo.NonNegativeReals, initialize = 0)
model.grid_export = pyo.Var(model.t, domain=pyo.NonNegativeReals, initialize = 0) 
model.is_importing = pyo.Var(model.t, domain=pyo.Binary, bounds=(0, 1), initialize=0)      
model.is_exporting = pyo.Var(model.t, domain=pyo.Binary, bounds=(0, 1), initialize=0)      

# --- Relax. variables  ---
model.Eminsocrelax = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0) 
model.Etargetrelax = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0) 
model.import_relax = pyo.Var(model.t, domain = pyo.NonNegativeReals, initialize = 0)  


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
        if int(round(pyo.value(m.type_[cp]))) == 1:  
            return m.PCP[cp,t] <= m.Pcpmax[cp] * m.cpa[cp,t] * m.alpha[ev,t]
        else:                 
            return m.PCP[cp,t] == m.Pcpmax[cp] * m.cpa[cp,t] * m.alpha[ev,t]
    return pyo.Constraint.Skip
model.conn_power_charge_limit_max = pyo.Constraint(model.ev, model.t, model.cp, rule = _conn_power_charging_limit_max)

# Min Charge 
def _conn_power_charging_limit_min(m, ev, t, cp): 
    if cp == int(round(pyo.value(m.cpconnected[ev]))):
        if int(round(pyo.value(m.type_[cp]))) == 1:  
            return m.PEV[ev,t] >= m.Pcpmin[cp] * m.cpa[cp,t] * m.alpha[ev,t]
    return pyo.Constraint.Skip
model.conn_power_charge_limit_min = pyo.Constraint(model.ev, model.t, model.cp, rule = _conn_power_charging_limit_min)

# V2G 
def _conn_power_discharging_limit_2(m, ev, t, cp): 
    if cp == int(round(pyo.value(m.cpconnected[ev]))): 
        if int(round(pyo.value(m.type_[cp]))) == 1:
            return m.PCP[cp,t] >= -m.Pcpmax[cp] * m.v2gcp[cp] * m.alpha[ev,t]
    return pyo.Constraint.Skip      
model.conn_power_discharge_limit_2 = pyo.Constraint(model.ev, model.t, model.cp, rule = _conn_power_discharging_limit_2)

# =============================================================================
# LIMITE DE POTÊNCIA DA ESTAÇÃO / GARAGEM (MONOFÁSICO)
# =============================================================================

# 1. Limite Máximo de Consumo (Carga)
def _limite_estacao_total_max(m, cs, t):
    evs_nesta_estacao = [ev for ev in m.ev if int(round(pyo.value(m.my_cs_id_cp[int(round(pyo.value(m.cpconnected[ev])))]))) == cs]
    if not evs_nesta_estacao:
        return pyo.Constraint.Skip
    potencia_estacao = sum(m.PEV[ev, t] - m.PEVdc[ev, t] for ev in evs_nesta_estacao)
    return potencia_estacao <= m.Pcsmax[cs]
model.limite_estacao_total_max = pyo.Constraint(model.cs, model.t, rule=_limite_estacao_total_max)

# 2. Limite Máximo de Injeção / V2G (Descarga)
def _limite_estacao_total_min(m, cs, t):
    evs_nesta_estacao = [ev for ev in m.ev if int(round(pyo.value(m.my_cs_id_cp[int(round(pyo.value(m.cpconnected[ev])))]))) == cs]
    if not evs_nesta_estacao:
        return pyo.Constraint.Skip
    potencia_estacao = sum(m.PEV[ev, t] - m.PEVdc[ev, t] for ev in evs_nesta_estacao)
    return potencia_estacao >= -m.Pcsmax[cs]
model.limite_estacao_total_min = pyo.Constraint(model.cs, model.t, rule=_limite_estacao_total_min)

# =============================================================================
# RESTRIÇÕES DOS VEÍCULOS ELÉTRICOS (EVs)
# =============================================================================

def _power_charging_limit(m,ev,t): 
    return m.PEV[ev,t] <= m.PchmaxEV[ev] * m.alpha[ev,t] * m.a[ev,t]
model.power_charging_limit2 = pyo.Constraint(model.ev, model.t, rule = _power_charging_limit)

def _power_discharging_limit(m,ev,t): 
    return m.PEVdc[ev,t] <= m.PdchmaxEV[ev] * m.alpha[ev,t] * m.b[ev,t] * m.v2gev[ev] 
model.power_discharging_limit2 = pyo.Constraint(model.ev, model.t, rule = _power_discharging_limit)

def _charging_discharging(m,ev,t): 
    return m.a[ev,t] + m.b[ev,t] <= 1 
model.charging_discharging = pyo.Constraint(model.ev, model.t, rule = _charging_discharging)

def _balance_energy_EVS(m,ev,t): 
    cp_id = int(round(pyo.value(m.cpconnected[ev])))
    eff_ch = m.evcheff[ev] * m.cheff[cp_id]
    eff_dch = m.evdcheff[ev] * m.dcheff[cp_id]
    
    if t == 1:
        return m.EEV[ev,t] == m.ESoc[ev] + m.PEV[ev,t]*m.dT[t]*eff_ch - m.PEVdc[ev,t]*m.dT[t]/eff_dch
    else:
        return m.EEV[ev,t] == m.EEV[ev,t-1] + m.PEV[ev,t]*m.dT[t]*eff_ch - m.PEVdc[ev,t]*m.dT[t]/eff_dch
model.balance_energy_EVS = pyo.Constraint(model.ev, model.t, rule = _balance_energy_EVS)

def _energy_limits_EVS_1(m,ev,t): 
    return m.EEV[ev,t] + m.Eminsocrelax[ev,t] >= m.EEVmin[ev]
model.energy_limits_EVS_1 = pyo.Constraint(model.ev, model.t, rule = _energy_limits_EVS_1)

def _energy_limits_EVS_2(m,ev,t): 
    return m.EEV[ev,t] <= m.EEVmax[ev] 
model.energy_limits_EVS_2 = pyo.Constraint(model.ev, model.t, rule = _energy_limits_EVS_2)  

def _balance_energy_EVS3(m,ev,t): 
    if t == m.t.last(): 
        return m.EEV[ev,t] + m.Etargetrelax[ev,t] >= m.EEVmax[ev] * m.target[ev] 
    return pyo.Constraint.Skip
model.balance_energy_EVS3 = pyo.Constraint(model.ev, model.t, rule = _balance_energy_EVS3)

# =============================================================================
# BESS constraints
# =============================================================================

def _max_charge_BESS(m, b, t):
    return m.PBess[b, t] <= m.bess_max_charge_rate[b] * m.bess_is_charging[b, t]
model.max_charge_BESS = pyo.Constraint(model.bat, model.t, rule=_max_charge_BESS)

def _max_discharge_BESS(m, b, t):
    return m.PBessdc[b, t] <= m.bess_max_discharge_rate[b] * m.bess_is_discharging[b, t]
model.max_discharge_BESS = pyo.Constraint(model.bat, model.t, rule=_max_discharge_BESS)

def _bess_status(m, b, t):
    return m.bess_is_charging[b, t] + m.bess_is_discharging[b, t] <= 1
model.bess_status = pyo.Constraint(model.bat, model.t, rule=_bess_status)

def _bess_energy_balance(m, b, t):
    if t == m.t.first(): 
        return m.EBess[b, t] == m.EBessInitial[b]*m.EBessMax[b] + m.PBess[b, t]*m.dT[t]*m.bess_charge_efficiency[b] - m.PBessdc[b, t]*m.dT[t]/m.bess_discharge_efficiency[b]
    else:
        return m.EBess[b, t] == m.EBess[b, t - 1] + m.PBess[b, t]*m.dT[t]*m.bess_charge_efficiency[b] - m.PBessdc[b, t]*m.dT[t]/m.bess_discharge_efficiency[b]
model.bess_energy_balance = pyo.Constraint(model.bat, model.t, rule=_bess_energy_balance)

def _bess_max_capacity(m, b, t):
    return m.EBess[b, t] <= m.EBessMax[b]
model.bess_max_capacity = pyo.Constraint(model.bat, model.t, rule=_bess_max_capacity)

def _bess_min_energy(m, b, t):
    return m.EBess[b, t] >= m.EBessMin[b]
model.bess_min_energy = pyo.Constraint(model.bat, model.t, rule=_bess_min_energy)

def _bess_target_energy(m, b, t):
    if t == m.t.last():
        return m.EBess[b, t] >= m.EBessMax[b] * 0.4
    return pyo.Constraint.Skip
model.bess_target_energy = pyo.Constraint(model.bat, model.t, rule=_bess_target_energy)

# =============================================================================
# BALANÇO GERAL DE ENERGIA E LIMITES DA REDE (MONOFÁSICO)
# =============================================================================

def _energy_balance(m, t): 
    ev_net_power = sum(m.PEV[ev,t] - m.PEVdc[ev,t] for ev in m.ev)
    bess_net_power = sum(m.PBess[b, t] - m.PBessdc[b, t] for b in m.bat)
    
    # Importação - Exportação = Consumo EVs + Consumo BESS + Cargas Instalação (pl) - Produção Solar (pv)
    return m.grid_import[t] - m.grid_export[t] == ev_net_power + bess_net_power + m.pl[t] - m.pv[t] 

model.energy_balance = pyo.Constraint(model.t, rule =_energy_balance)

def _contracted_power_constraint(m, t): 
    return m.grid_import[t] <= m.pt[t] * m.is_importing[t] + m.import_relax[t]
model.contracted_power_constraint = pyo.Constraint(model.t, rule =_contracted_power_constraint) 

def _contracted_power_constraint2(m, t): 
    return m.grid_export[t] <= m.pt[t] * m.is_exporting[t] 
model.contracted_power_constraint2 = pyo.Constraint(model.t, rule =_contracted_power_constraint2)  

def _importing_exporting(m, t): 
    return m.is_importing[t] + m.is_exporting[t] <= 1
model.importing_exporting = pyo.Constraint(model.t, rule =_importing_exporting)


#************************************************************************Objective Function***********************************************************

def _FOag(m):
    # 1. Custos da Rede (Monofásico)
    custos_rede = sum(
        (m.grid_import[t] * m.dT[t] * m.import_price[t]) 
        - (m.grid_export[t] * m.dT[t] * m.export_price[t]) 
        + (m.import_relax[t] * m.dT[t] * m.pc_penalty)
        for t in np.arange(1, n_time + 1)
    )
    
    # 2. Custos e Penalizações dos EVs
    custos_evs = sum(
        (m.PEVdc[ev,t] * m.dT[t] * m.export_price[t] * m.DegCost)
        + ((m.EEVmax[ev] * m.target[ev]) - m.EEV[ev,t]) * m.m
        + (m.Eminsocrelax[ev,t] * m.penalty1)
        + (m.Etargetrelax[ev,t] * m.penalty2)
        for ev in np.arange(1, n_evs + 1) 
        for t in np.arange(1, n_time + 1)
    )
    
    return custos_rede + custos_evs

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
    s = pd.Series(vals.extract_values(), index=vals.extract_values().keys())
    if type(s.index[0]) == tuple:    
        s = s.unstack(level=1)
    else:
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

# O import/export agora já sai numa única coluna, não precisa de transposição
grid_import_df = ext_pyomo_vals(model.grid_import) 
grid_export_df = ext_pyomo_vals(model.grid_export)

PBess_df = ext_pyomo_vals(model.PBess)
PBessdc_df = ext_pyomo_vals(model.PBessdc)
EBess_df = ext_pyomo_vals(model.EBess)
bess_charging_df = ext_pyomo_vals(model.bess_is_charging)
bess_discharging_df = ext_pyomo_vals(model.bess_is_discharging)

EEVmax_df = ext_pyomo_vals(model.EEVmax)
Etargetrelax_df = ext_pyomo_vals(model.Etargetrelax) 
Eminsocrelax_df = ext_pyomo_vals(model.Eminsocrelax)


charge_cost = sum([PEV_df[t][ev]*dT_df[0][t]*import_price_df[0][t]
                   for ev in np.arange(1, n_evs + 1) for t in np.arange(1, n_time + 1)])

discharge_cost = sum([PEVdc_df[t][ev]*dT_df[0][t]*import_price_df[0][t]
                      for ev in np.arange(1, n_evs + 1) for t in np.arange(1, n_time + 1)])


print('Charge cost: {}'.format(charge_cost))
print('Discharge cost: {}'.format(discharge_cost))

print("Total Charge: {}".format(np.sum(PEV_df.to_numpy())))
print("Total Discharge: {}".format(np.sum(PEVdc_df.to_numpy())))

# =============================================================================
# CÁLCULO DO CUSTO REAL DA ENERGIA (SEM PENALIZAÇÕES)
# =============================================================================
custo_real_energia = sum(
    ((pyo.value(model.grid_import[t]) / 1000.0) * pyo.value(model.dT[t]) * pyo.value(model.import_price[t])) -
    ((pyo.value(model.grid_export[t]) / 1000.0) * pyo.value(model.dT[t]) * pyo.value(model.export_price[t]))
    for t in model.t
)

print("\n" + "="*50)
print(f"CUSTO REAL DA ENERGIA (Pyomo Day-Ahead): {custo_real_energia:.4f} €")
print("="*50 + "\n")

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

PEVdc_df.to_csv(folder + '/PEVdc.csv')
PCPdc_df.to_csv(folder + '/PCPdc.csv')
PEV_df.sum().to_csv(folder + '/PEV_h.csv')
PEVdc_df.sum().to_csv(folder + '/PEVdc_h.csv')

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


# Creating a DataFrame to calculate EV accounts
EEVmax_values = EEVmax_df.values.tolist()
EEVmax_values = [value for sublist in EEVmax_values for value in sublist]

ev_accounts = pd.DataFrame([[(EEVmax_values[i] - EEV_df.iloc[i, j]) * 0.1 for j in range(len(EEV_df.columns))] for i in range(len(EEVmax_values))], columns=EEV_df.columns)
ev_accounts.to_csv(folder + '/ev_accounts.csv')


# =============================================================================
# EXTRACÇÃO DOS DADOS PARA EXCEL
# =============================================================================

# 1. Iniciar o dicionário para criar o DataFrame final
excel_dict = {}

# --- PREÇOS E BESS (BATERIA) GLOBAL ---
excel_dict['Preco_Importacao'] = [pyo.value(model.import_price[t]) for t in model.t]
excel_dict['Preco_Exportacao'] = [pyo.value(model.export_price[t]) for t in model.t]

# --- DADOS DA REDE E INSTALAÇÃO (Monofásico) ---
excel_dict['Rede_Importacao_kW'] = [pyo.value(model.grid_import[t]) for t in model.t]
excel_dict['Rede_Exportacao_kW'] = [pyo.value(model.grid_export[t]) for t in model.t]
excel_dict['Instalacao_PL_kW'] = [pyo.value(model.pl[t]) for t in model.t]
excel_dict['Solar_PV_kW'] = [pyo.value(model.pv[t]) for t in model.t]

# --- DADOS DA BATERIA ESTACIONÁRIA ---
excel_dict['BESS_Carga_kW'] = [pyo.value(sum(model.PBess[b, t] for b in model.bat)) for t in model.t]
excel_dict['BESS_Descarga_kW'] = [pyo.value(sum(model.PBessdc[b, t] for b in model.bat)) for t in model.t]
excel_dict['Energia_Armazenada_BESS_kWh'] = [pyo.value(sum(model.EBess[b, t] for b in model.bat)) for t in model.t]

# --- DADOS POR VEÍCULO ELÉTRICO ---
for ev in model.ev:
    excel_dict[f'VE{ev}_Ligado'] = [pyo.value(model.alpha[ev, t]) for t in model.t]
    excel_dict[f'VE{ev}_a_Carregar'] = [pyo.value(model.a[ev, t]) for t in model.t]
    excel_dict[f'VE{ev}_a_Descarregar'] = [pyo.value(model.b[ev, t]) for t in model.t]
    excel_dict[f'VE{ev}_Carga_kW'] = [pyo.value(model.PEV[ev, t]) for t in model.t]
    excel_dict[f'VE{ev}_Descarga_kW'] = [pyo.value(model.PEVdc[ev, t]) for t in model.t]
    excel_dict[f'VE{ev}_Energia_kWh'] = [pyo.value(model.EEV[ev, t]) for t in model.t]
    excel_dict[f'VE{ev}_Relaxamentos'] = [pyo.value(model.Eminsocrelax[ev, t] + model.Etargetrelax[ev, t]) for t in model.t]

# 2. Criar DataFrame e Exportar para Excel
fluxos_df = pd.DataFrame(excel_dict, index=list(model.t))
fluxos_df.index.name = 'Instante'

fluxos_df.to_excel(folder + '/Gestao_Energia_e_Veiculos_Final_Monofasico.xlsx', engine='openpyxl')

print("\nFicheiro Excel detalhado gerado com Sucesso! ")

# =============================================================================
# GERAÇÃO AUTOMÁTICA DE GRÁFICOS (Monofásico)
# =============================================================================
print("\nA gerar o gráfico final de balanço de potência...")

# Criar a figura única
fig, ax = plt.subplots(figsize=(14, 6))
fig.suptitle('Balanço de Potência (Sistema Monofásico)', fontsize=18, fontweight='bold')

time_steps = list(range(1, n_time + 1))

# Extração de dados globais
net_grid = [pyo.value(model.grid_import[t] - model.grid_export[t]) for t in model.t]
net_bess = [pyo.value(sum(model.PBess[b, t] - model.PBessdc[b, t] for b in model.bat)) for t in model.t]
net_ev = [pyo.value(sum(model.PEV[ev, t] - model.PEVdc[ev, t] for ev in model.ev)) for t in model.t]
pl_vals = [pyo.value(model.pl[t]) for t in model.t]
pv_vals = [-pyo.value(model.pv[t]) for t in model.t]

# Limite Contratado
pt_vals_pos = [pyo.value(model.pt[t]) for t in model.t]
pt_vals_neg = [-pyo.value(model.pt[t]) for t in model.t]

# --- DESENHAR AS LINHAS ---
ax.plot(time_steps, pl_vals, label='Consumo Base', color='black', linewidth=1, alpha=0.6)
ax.fill_between(time_steps, 0, pv_vals, label='Produção Solar', color='orange', alpha=0.3)

# Limites da Rede
ax.plot(time_steps, pt_vals_pos, label='Limite Import/Export', color='red', linestyle='--', linewidth=1.5)
ax.plot(time_steps, pt_vals_neg, color='red', linestyle='--', linewidth=1.5)

ax.plot(time_steps, net_grid, label='Rede Líquida', color='dodgerblue', linewidth=2)
ax.plot(time_steps, net_bess, label='BESS Líquido', color='purple', linewidth=2, marker='o', markersize=3)
ax.plot(time_steps, net_ev, label='VEs Líquido', color='crimson', linewidth=2, marker='s', markersize=3)

# Estilização
ax.set_ylabel('Potência (W)')
ax.set_xlabel('Hora do Dia (h)')
ax.grid(True, linestyle=':', alpha=0.6)
ax.axhline(0, color='black', linewidth=1)
ax.set_xlim(1, 24)
ax.set_xticks(time_steps)
ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0., fontsize='small')

plt.tight_layout()

plot_path = os.path.join(folder, 'Grafico_Final_Balanço_Monofasico.png')
plt.savefig(plot_path, dpi=300, bbox_inches='tight')
print(f"Gráfico atualizado guardado em: {plot_path}")