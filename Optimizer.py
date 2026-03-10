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

# Variables representing time, electric vehicles, charging points, and shared stations.
n_time = data['energy_price']['dT'].size
n_evs = data['evs_inputs']['Esoc'].size
cp = data['cp_inputs']['cs_id'].size
css = data['css_inputs']['cs_id'].size
fases = data['fases']['line'].size

print(f"\nEVs: {n_evs}\nCharging Station {css}\nCharging Points: {cp}\nPhases: {fases}")

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

#***************************************Parameters definition************************************
model.pt = pyo.Param(model.f, model.t, initialize =_auxDictionary(data['pt'].to_numpy()))
model.pv = pyo.Param(model.f, model.t, initialize =_auxDictionary(data['pv'].to_numpy()))
model.ev_id = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,10]))
model.cs_id = pyo.Param(model.cs, initialize =_auxDictionary(data['css_inputs'].to_numpy()[:,0]))
model.my_cs_id_cp = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,0]))
model.cp_id = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,10]))
model.csconnected = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,11]))
model.ESoc = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,0]))
model.EEVmin = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,1]))
model.EEVmax = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,2]))
model.Etrip = pyo.Param(model.ev, initialize=_auxDictionary(data['evs_inputs'].to_numpy()[:,3]))
model.pl = pyo.Param(model.f, model.t, initialize =_auxDictionary(data['pl'].to_numpy()))
model.PchmaxEV = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,4]))
model.PdchmaxEV = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,5]))
model.evcheff = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,6])) 
model.evdcheff = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,7])) 
model.cheff = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,1])) 
model.dcheff = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,2])) 
model.cpconnected = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,8])) 
model.Pcpmax = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,3])) 
model.Pcpmin = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,5])) 
#model.Pcpmax = pyo.Param(model.f, model.cp, initialize = _auxDictionary(data['cps_power'].to_numpy()))
#model.Pcpdis = pyo.Param(model.cp, initialize =_auxDictionary(data['CSs_inputs'].to_numpy()[:,5])) 

model.type_ = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,9])) # If the CP is controlable or not (on-off socket)
model.v2gcp = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,4])) 
model.v2gev = pyo.Param(model.ev, initialize =_auxDictionary(data['evs_inputs'].to_numpy()[:,9])) 
#model.Pcsmax = pyo.Param(model.cs, initialize =_auxDictionary(data['css_inputs'].to_numpy()[:,1])) # In case of being free from becoming unbalanced.
model.Pcsmax = pyo.Param(model.f, model.cs, initialize = _auxDictionary(data['css_power'].to_numpy())) # To keep it linked and balanced.

#cp_inputs_as_int = data['cp_inputs'].to_numpy().astype(int)
model.place = pyo.Param(model.cp, initialize =_auxDictionary(data['cp_inputs'].to_numpy()[:,6])) 

model.dT = pyo.Param(model.t, initialize =_auxDictionary(data['energy_price'].to_numpy()[:,0]))
model.import_price = pyo.Param(model.t, initialize =_auxDictionary(data['energy_price'].to_numpy()[:,1]))
model.export_price = pyo.Param(model.t, initialize =_auxDictionary(data['energy_price'].to_numpy()[:,2]))
model.S = pyo.Param(model.ev, model.t, initialize = _auxDictionary(data['S'].to_numpy()))
model.alpha = pyo.Param(model.ev, model.t, initialize = _auxDictionary(data['alpha'].to_numpy()))
model.my_cp_fases = pyo.Param(model.cp, initialize=_auxDictionary(data['cp_inputs'].to_numpy()[:,8]))
model.penalty1 = 1000000
model.penalty2 = 1000000
model.penalty3 = 0.6
model.DegCost = 0.10
model.m = pyo.Param(model.ev, initialize=_auxDictionary(data['evs_inputs'].to_numpy()[:,11]))


#***************************************Variables definition********************
model.PEV = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0)
model.PEVdc = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0)
model.EEV = pyo.Var(model.ev, model.t, domain = pyo.Reals, initialize = 0)
model.Etriprelax = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0)
model.Eminsocrelax = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0)
model.Etargetrelax = pyo.Var(model.ev, model.t, domain = pyo.NonNegativeReals, initialize = 0)
model.Etripn = pyo.Var(model.ev, model.t, domain = pyo.Reals, initialize = 0)
model.a = pyo.Var(model.ev, model.t, domain = pyo.Binary,bounds=(0, 1), initialize=0) #EV charging binary 
model.b = pyo.Var(model.ev, model.t, domain = pyo.Binary,bounds=(0, 1), initialize=0) #EV discharging binary 
model.PCP = pyo.Var(model.cp, model.t, domain = pyo.Reals, initialize = 0)
model.PCPdc = pyo.Var(model.cp, model.t, domain = pyo.Reals, initialize = 0)
#model.PCS = pyo.Var(model.cs, model.t, domain = pyo.Reals, initialize = 0)
model.PCS = pyo.Var(model.f, model.cs, model.t, domain = pyo.Reals, initialize = 0)
model.PCSaux = pyo.Var(model.f, model.t, domain = pyo.Reals, initialize = 0)
model.grid_import = pyo.Var(model.f,model.t, domain=pyo.NonNegativeReals, initialize = 0)
model.grid_export = pyo.Var(model.f,model.t, domain=pyo.NonNegativeReals, initialize = 0)
model.is_importing = pyo.Var(model.t, domain=pyo.Binary, bounds=(0, 1), initialize=0)
model.is_exporting = pyo.Var(model.t, domain=pyo.Binary, bounds=(0, 1), initialize=0)
model.import_relax = pyo.Var(model.f,model.t, domain=pyo.NonNegativeReals, initialize = 0)
model.export_relax = pyo.Var(model.f,model.t, domain=pyo.NonNegativeReals, initialize = 0)
model.cpa = pyo.Var(model.cp, model.t, domain=pyo.Binary, bounds=(0, 1), initialize=0)




#****************************************************Connectors constraints******************************************************
# Power consumption of each Connectors related to each EV charging and discharging connected to its  
def _conn_power_consumption(m, ev, t, cp): 
    if cp == int(round(pyo.value(m.cpconnected[ev]))):
        return m.PCP[cp,t] == m.PEV[ev,t] - m.PEVdc[ev,t]
    return pyo.Constraint.Skip
model._conn_power_consumption = pyo.Constraint(model.ev, model.t, model.cp, rule = _conn_power_consumption)

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
            return m.PCP[cp,t] >= m.Pcpmin[cp] * m.cpa[cp,t] * m.alpha[ev,t]
    return pyo.Constraint.Skip
model.conn_power_charge_limit_min = pyo.Constraint(model.ev, model.t, model.cp, rule = _conn_power_charging_limit_min)

# V2G
def _conn_power_discharging_limit_2(m, ev, t, cp): 
    if cp == m.cpconnected[ev]: 
        if m.type_[cp] == 1:
            return m.PCPdc[cp,t] >= -m.Pcpmax[cp] * m.v2gcp[cp] * m.alpha[ev,t]
    return pyo.Constraint.Skip      
model.conn_power_discharge_limit_2 = pyo.Constraint(model.ev, model.t, model.cp, rule = _conn_power_discharging_limit_2)

# =============================================================================
# LIMITE DE POTÊNCIA DA ESTAÇÃO / GARAGEM (Por Fase e Por Estação)
# Impede a garagem de ultrapassar os 6.9 kW totais (2.3 kW / 2300 W por fase)
# =============================================================================
def _limite_estacao_fase(m, f, cs, t):
    potencia_fase = 0
    carros_nesta_fase = 0
    
    for ev in m.ev:
        # Pega no Ponto de Carregamento (CP) ao qual o veículo está ligado
        cp_of_ev = int(round(pyo.value(m.cpconnected[ev])))
        
        # Pega na estação e na fase correspondente desse CP
        cs_of_cp = int(round(pyo.value(m.my_cs_id_cp[cp_of_ev])))
        fase_of_cp = int(round(pyo.value(m.my_cp_fases[cp_of_ev])))
        
        # Se o CP pertence a esta garagem (cs) e está ligado a esta fase (f)
        if cs_of_cp == cs and fase_of_cp == f:
            potencia_fase += (m.PEV[ev, t] - m.PEVdc[ev, t])
            carros_nesta_fase += 1
            
    # Se não houver nenhum posto ligado a esta fase, salta a restrição para essa fase
    if carros_nesta_fase == 0:
        return pyo.Constraint.Skip
        
    # Garante que a soma da potência nesta fase não ultrapassa os 2300 W
    return potencia_fase <= m.Pcsmax[f, cs]

# Ativamos a restrição no modelo
model.limite_estacao_fase_rule = pyo.Constraint(model.f, model.cs, model.t, rule=_limite_estacao_fase)

#****************************************************EV constraints******************************************************
# EV power consumption constraints 
def _power_charging_limit1(m,ev,t): 
    return m.PEV[ev,t] >= 0
model.power_charging_limit1 = pyo.Constraint(model.ev, model.t, rule = _power_charging_limit1)

# EV power consumption constraints
def _power_charging_limit2(m,ev,t): 
    return m.PEV[ev,t] <= m.PchmaxEV[ev]*m.alpha[ev,t]*m.a[ev,t]
model.power_charging_limit2 = pyo.Constraint(model.ev, model.t, rule = _power_charging_limit2)

def _power_discharging_limit1(m,ev,t):
    return m.PEVdc[ev,t] >= 0
model.power_discharging_limit1 = pyo.Constraint(model.ev, model.t, rule = _power_discharging_limit1)

# EV power discharging constraints 
def _power_discharging_limit2(m,ev,t): 
    return m.PEVdc[ev,t] <= m.PdchmaxEV[ev]*m.alpha[ev,t]*m.b[ev,t]*m.v2gev[ev] 
model.power_discharging_limit2 = pyo.Constraint(model.ev, model.t, rule = _power_discharging_limit2)

# EV charging and discharging binary limitation 
def _charging_discharging(m,ev,t): 
    return m.a[ev,t] + m.b[ev,t] <= 1 
model.charging_discharging = pyo.Constraint(model.ev, model.t, rule = _charging_discharging)

# EV energy balance at time 0.
def _balance_energy_EVS(m,ev,t,cp): 
    if t == 1:
        #return m.EEV[ev,t] - m.Etriprelax[ev,t] == m.ESoc[ev] + m.PEV[ev,t]*m.dT[t]*(m.evcheff[ev]*m.cheff[cp]) - m.PEVdc[ev,t]*m.dT[t]/(m.evcheff[ev]*m.cheff[cp]) - m.Etripn[ev,t]
        return m.EEV[ev,t] == m.ESoc[ev] + m.PEV[ev,t]*m.dT[t]*m.evcheff[ev] - m.PEVdc[ev,t]*m.dT[t]/(m.evcheff[ev]*m.cheff[cp])
    
    elif t > 1:
        #return m.EEV[ev,t] - m.Etriprelax[ev,t] == m.EEV[ev,t-1] + m.PEV[ev,t]*m.dT[t]*m.evcheff[ev] - m.PEVdc[ev,t]*m.dT[t]/m.evcheff[ev] - m.Etripn[ev,t]
        return m.EEV[ev,t] == m.EEV[ev,t-1] + m.PEV[ev,t]*m.dT[t]*m.evcheff[ev] - m.PEVdc[ev,t]*m.dT[t]/m.evcheff[ev]
model.balance_energy_EVS = pyo.Constraint(model.ev, model.t, model.cp, rule = _balance_energy_EVS)

# EV minimum capacity limitation.
def _energy_limits_EVS_1(m,ev,t): 
    return m.EEV[ev,t] + m.Eminsocrelax[ev,t] >= m.EEVmin[ev]
    #return m.EEV[ev,t] >= m.EEVmin[ev]
model.energy_limits_EVS_1 = pyo.Constraint(model.ev, model.t, rule = _energy_limits_EVS_1)

# EV maximum capacity limitation.
def _energy_limits_EVS_2(m,ev,t): 
    return m.EEV[ev,t] <= m.EEVmax[ev] 
model.energy_limits_EVS_2 = pyo.Constraint(model.ev, model.t, rule = _energy_limits_EVS_2)  

# Target.
def _balance_energy_EVS3(m,ev,t): 
    if t == 24: #Isto tem que ver a hora de partida do carro (?) no lugar do 24, ou seja vamos por na bateria um target para quando ele deixar o parque
        #return m.EEV[ev,t] + m.Etriprelax[ev,t] >= m.EEVmax[ev]*m.m[ev]
        return m.EEV[ev,t] + m.Etargetrelax[ev,t] >= m.EEVmax[ev]*m.m[ev] #declarar a variavel de relax
    return pyo.Constraint.Skip
model.balance_energy_EVS3 = pyo.Constraint(model.ev, model.t, rule = _balance_energy_EVS3)


#***********************************************BESS constraints********************************************

def max_charge_BESSaux(m, t):
    return m.PBess_aux[t] == m.bess_max_charge_rate * m.bess_is_charging[t]

  # BESS charge rate (in W)
def max_charge_BESS(m, t):
    return m.PBess[t] <= m.PBess_aux[t]

def max_discharge_BESSaux(m, t):
    return m.PBessdc_aux[t] == m.bess_max_discharge_rate * m.bess_is_discharging[t]

# BESS discharge rate (in W)
def max_discharge_BESS(m, t):
    return m.PBessdc[t] <= m.PBessdc_aux[t]

# BESS status
def bess_status(m, t):
    return m.bess_is_charging[t] + m.bess_is_discharging[t] <= 1

# BESS energy balance in Wh
def bess_energy_balance(m, t):
    if t > 1:
        return m.EBess[t] == m.EBess[t - 1] + (m.PBess[t]*m.dT)* m.bess_charge_efficiency  - (m.PBessdc[t]*m.dT)/ m.bess_discharge_efficiency
    return m.EBess[t]  == m.EBessInitial + (m.PBess[t]*m.dT)* m.bess_charge_efficiency  - (m.PBessdc[t]*m.dT)/ m.bess_discharge_efficiency

# BESS maximum capacity in Wh
def bess_max_capacity(m, t):
    return m.EBess[t] <= m.EBessMax

# BESS minimum capacity in Wh
def bess_min_energy(m, t):
    if t == m.t.last():
        return m.EBess[t] >= m.EBessMax * 0.4
    return m.EBess[t] >= m.EBessMin

#Energy balance in the system considering threephase balanced PV, threephase unbalanced load consumption, and threphase unbalanced CS 
#def _energy_balance(m,f,t): 
#    #return m.grid_import[t]  == sum(m.PCS[cs,t] for cs in m.cs)
#    return m.grid_import[f,t] - model.grid_export[f,t]  == sum([m.PEV[ev,t] - m.PEVdc[ev,t] for ev in m.ev]) + m.pl[f,t] - m.pv[f,t] 
#model._energy_balance = pyo.Constraint(model.f, model.t, rule =_energy_balance)  


def _energy_balance(m,f,t): 
    # Dividimos o consumo total dos EVs por 3 para espalhar a carga equilibradamente pelas 3 fases
    ev_net_power = sum([m.PEV[ev,t] - m.PEVdc[ev,t] for ev in m.ev]) / len(m.f)
    return m.grid_import[f,t] - m.grid_export[f,t] == ev_net_power + m.pl[f,t] - m.pv[f,t] 
model._energy_balance = pyo.Constraint(model.f, model.t, rule =_energy_balance)

# Contracted power limitation
def _contracted_power_constraint(m,f,t): 
    # Usar a variável import_relax para dar margem de manobra ao solver
    return m.grid_import[f,t] <= m.pt[f,t]*m.is_importing[t] + m.import_relax[f,t]
model._contracted_power_constraint = pyo.Constraint(model.f, model.t, rule =_contracted_power_constraint) 

def _contracted_power_constraint2(m,f,t): 
    #return m.grid_export[f,t]  <= m.pt[f,t]*m.is_exporting[t] + m.export_relax[f,t]
    return m.grid_export[f,t]  <= m.pt[f,t]*m.is_exporting[t] 
model._contracted_power_constrain2 = pyo.Constraint(model.f, model.t, rule =_contracted_power_constraint2)  


def _importing_exporting(m,t): 
    return m.is_importing[t] + m.is_exporting[t]  <= 1
model._importing_exporting= pyo.Constraint(model.t, rule =_importing_exporting)  


#************************************************************************Objective Function***********************************************************
def _FOag(m):
    return sum(m.grid_import[f,t] *(m.import_price[t]) - m.grid_export[f,t] *(m.export_price[t]) + (m.EEVmax[ev] - m.EEV[ev,t])*0.1 + m.import_relax[f,t]*0.1 + m.Eminsocrelax[ev,t]*m.penalty2 for ev in np.arange(1, n_evs + 1) for f in np.arange(1, fases + 1) for t in np.arange(1, n_time + 1))     
 
model.FOag = pyo.Objective(rule = _FOag, sense = pyo.minimize)

#************************************************************************Solve the model***********************************************************
from pyomo.opt import SolverFactory
model.write('res_V4_EC.lp',  io_options={'symbolic_solver_labels': True})

opt = pyo.SolverFactory('cplex', executable='C:\\Program Files\\IBM\\ILOG\\CPLEX_Studio2211\\cplex\\bin\\x64_win64\\cplex.exe')
opt.options['LogFile'] = 'res_V4_EC.log'

results = opt.solve(model)#, tee=True)
results.write()

#************************************************************************End Time information***********************************************************
pyo.value(model.FOag)

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
PCS_df = ext_pyomo_vals(model.PCS)
PCSaux_df = ext_pyomo_vals(model.PCSaux)
dT_df = ext_pyomo_vals(model.dT)
import_price_df = ext_pyomo_vals(model.import_price)
export_price_df = ext_pyomo_vals(model.export_price)
EEV_df = ext_pyomo_vals(model.EEV)
grid_import_df = ext_pyomo_vals(model.grid_import) 
grid_export_df = ext_pyomo_vals(model.grid_export)

# Extracting three-phase data and organizing into separate columns:

# Import
#grid_import_df_3colums = pd.DataFrame(np.reshape(grid_import_df.values,(24, 3)), index=range(1,25))
#grid_import_df_3colums.columns = ['grid_import_ph1', 'grid_import_ph2', 'grid_import_ph3']

# Export
#grid_export_df_3colums = pd.DataFrame(np.reshape(grid_export_df.values, (24, 3)), index=range(1,25))
#grid_export_df_3colums.columns = ['grid_export_ph1', 'grid_export_ph2', 'grid_export_ph3']

# Import (usando .T para transpor a tabela corretamente)
grid_import_df_3colums = pd.DataFrame(grid_import_df.values.T, index=range(1,25))
grid_import_df_3colums.columns = ['grid_import_ph1', 'grid_import_ph2', 'grid_import_ph3']

# Export (usando .T para transpor a tabela corretamente)
grid_export_df_3colums = pd.DataFrame(grid_export_df.values.T, index=range(1,25))
grid_export_df_3colums.columns = ['grid_export_ph1', 'grid_export_ph2', 'grid_export_ph3']

EEVmax_df = ext_pyomo_vals(model.EEVmax)
Etriprelax_df = ext_pyomo_vals(model.Etriprelax)
Eminsocrelax_df = ext_pyomo_vals(model.Eminsocrelax)
Etripn_df = ext_pyomo_vals(model.Etripn)


#return sum(m.grid_import[f,t] *(m.import_price[t]) - m.grid_export[f,t]*(m.export_price[t]) + (m.EEVmax[ev] - m.EEV[ev,t])*0.1  + m.Eminsocrelax[ev,t]*m.penalty2 for ev in np.arange(1, n_evs + 1) for f in np.arange(1, fases + 1) for t in np.arange(1, n_time + 1)) 

#second_term = sum([(EEVmax_df - EEV_df[0][t])*0.1  for t in np.arange(1, n_time + 1)])

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
    
EEV_df.to_csv(folder + '/EEV.csv')
EEVmax_df.to_csv(folder + '/EEVmax.csv')
PEV_df.to_csv(folder + '/PEV.csv')
PCP_df.to_csv(folder + '/PCP.csv')
PCS_df.to_csv(folder + '/PCS.csv')
PCSaux_df.to_csv(folder + '/PCSaux.csv')
grid_import_df.to_csv(folder + '/grid_import.csv')
grid_export_df.to_csv(folder + '/grid_export.csv')
import_price_df.to_csv(folder + '/import_price.csv')
export_price_df.to_csv(folder + '/export_price.csv')

PEVdc_df.to_csv(folder + '/PEVdc.csv')
PCPdc_df.to_csv(folder + '/PCPdc.csv')
PEV_df.sum().to_csv(folder + '/PEV_h.csv')
PEVdc_df.sum().to_csv(folder + '/PEVdc_h.csv')
grid_import_df.sum().to_csv(folder + '/grid_import_h.csv')
grid_import_df_3colums.to_csv(folder + '/grid_import_per_phase.csv')

grid_export_df.sum().to_csv(folder + '/grid_export_h.csv')
grid_export_df_3colums.to_csv(folder + '/grid_export_per_phase.csv')

Etriprelax_df.to_csv(folder + '/Etriprelax.csv')
Etriprelax_df.sum().to_csv(folder + '/Etriprelax_h.csv')

Eminsocrelax_df.to_csv(folder + '/Eminsocrelax.csv')
Eminsocrelax_df.sum().to_csv(folder + '/Eminsocrelax_h.csv')

Etripn_df.to_csv(folder + '/Etripn.csv')
Etripn_df.sum().to_csv(folder + '/Etripn_h.csv')

# Creating a CSV with the grid accounts, combining import and export prices along with three-phase import and export data
grid_accounts = pd.concat([import_price_df, export_price_df, grid_import_df_3colums, grid_export_df_3colums], axis=1)


######################################
# Cria uma lista com os nomes atuais
novos_nomes = list(grid_accounts.columns)

# Muda os nomes na lista
novos_nomes[0] = 'import_price'
novos_nomes[1] = 'export_price'

# Devolve a lista à tabela
grid_accounts.columns = novos_nomes

###################
# Renaming the colums
#grid_accounts.columns.values[0] = 'import_price'
#grid_accounts.columns.values[1] = 'export_price'
#######################
# Doing the accounts
grid_accounts['grid_import_ph1*import_price'] = grid_accounts['grid_import_ph1'] * grid_accounts['import_price']
grid_accounts['grid_import_ph2*import_price'] = grid_accounts['grid_import_ph2'] * grid_accounts['import_price']
grid_accounts['grid_import_ph3*import_price'] = grid_accounts['grid_import_ph3'] * grid_accounts['import_price']
grid_accounts['total_grid_import*import_price'] = grid_accounts['grid_import_ph1*import_price'] + grid_accounts['grid_import_ph2*import_price'] + grid_accounts['grid_import_ph3*import_price']

grid_accounts['grid_export_ph1*export_price'] = grid_accounts['grid_export_ph1'] * grid_accounts['export_price']
grid_accounts['grid_export_ph2*export_price'] = grid_accounts['grid_export_ph2'] * grid_accounts['export_price']
grid_accounts['grid_export_ph3*export_price'] = grid_accounts['grid_export_ph3'] * grid_accounts['export_price']
grid_accounts['total_grid_export*export_price'] = grid_accounts['grid_export_ph1*export_price'] + grid_accounts['grid_export_ph2*export_price'] + grid_accounts['grid_export_ph3*export_price']

grid_accounts.to_csv(folder + '/grid_accounts.csv')

# Creating a CSV with the penalty for excessive trip duration, calculated as the product of trip duration and penalty coefficient
etrip_penalty = Etripn_df * model.penalty2
etrip_penalty.to_csv(folder + '/etrip_times_penalty.csv')


# Creating a DataFrame to calculate EV accounts, subtracting actual energy values from maximum energy values and applying a 10% fee
EEVmax_values = EEVmax_df.values.tolist()
EEVmax_values = [value for sublist in EEVmax_values for value in sublist]

ev_accounts = pd.DataFrame([[(EEVmax_values[i] - EEV_df.iloc[i, j]) * 0.1 for j in range(len(EEV_df.columns))] for i in range(len(EEVmax_values))], columns=EEV_df.columns)
ev_accounts.to_csv(folder + '/ev_accounts.csv')


# =============================================================================
# EXTRACÇÃO DOS DADOS PARA EXCEL (Fluxos de Potência + Preços + Estado dos VEs)
# =============================================================================

# 1. Extrair Variáveis de Potência
pl_df = ext_pyomo_vals(model.pl)
pv_df = ext_pyomo_vals(model.pv)

# 2. Extrair Variáveis de Estado (Disponibilidade e Ação dos VEs)
alpha_df = ext_pyomo_vals(model.alpha) # Disponibilidade (plugged-in)
a_df = ext_pyomo_vals(model.a)         # A carregar (binário)
b_df = ext_pyomo_vals(model.b)         # A descarregar (binário)

# 3. Agregações (Somar fases e VEs)
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

# Correção das Fases da Rede
grid_import_df_3colums = pd.DataFrame(grid_import_df.values.T, index=range(1,25))
grid_export_df_3colums = pd.DataFrame(grid_export_df.values.T, index=range(1,25))
total_grid_import = grid_import_df_3colums.sum(axis=1)
total_grid_export = grid_export_df_3colums.sum(axis=1)

# Extrair Preços
preco_imp = import_price_df.values.flatten()
preco_exp = export_price_df.values.flatten()

# 4. Criar a Tabela Final Consolidada
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
    'Instalacao_PL_kW': total_pl.values,
    'Solar_PV_kW': total_pv.values
})

# Definir o Instante como índice
fluxos_df.set_index('Instante', inplace=True)

# =============================================================================
# EXTRACÇÃO DE ENERGIAS E RELAXAMENTOS PARA EXCEL
# =============================================================================

# 1. Extrair Variáveis de Energia (SOC) e Relaxamentos
EEV_df = ext_pyomo_vals(model.EEV)
Eminsocrelax_df = ext_pyomo_vals(model.Eminsocrelax)
Etargetrelax_df = ext_pyomo_vals(model.Etargetrelax) # Penalização de partida
Etriprelax_df = ext_pyomo_vals(model.Etriprelax)

# 2. Somar os valores de todos os VEs para o total do parque
if len(EEV_df.index) == n_time:
    total_soc_kwh = EEV_df.sum(axis=1) 
    total_relax_minsoc = Eminsocrelax_df.sum(axis=1)
    total_relax_target = Etargetrelax_df.sum(axis=1)
    total_relax_trip = Etriprelax_df.sum(axis=1)
else:
    total_soc_kwh = EEV_df.sum(axis=0)
    total_relax_minsoc = Eminsocrelax_df.sum(axis=0)
    total_relax_target = Etargetrelax_df.sum(axis=0)
    total_relax_trip = Etriprelax_df.sum(axis=0)

# Criar uma coluna que soma todos os erros/penalizações
total_erros_relaxamento = total_relax_minsoc + total_relax_target + total_relax_trip

# 3. Adicionar as novas colunas ao teu fluxos_df existente (assumindo que o código anterior correu)
fluxos_df['Energia_Armazenada_VEs_kWh'] = total_soc_kwh.values
fluxos_df['Falhas_Relaxamento_kWh'] = total_erros_relaxamento.values

# =============================================================================
# DEFINIÇÃO DOS MODOS DE OPERAÇÃO
# =============================================================================
def definir_modo_operacao_equacoes(row, p_grid_max):
    # Tolerância para lidar com imprecisões numéricas do solver
    tol = 0.001
    
    # 1. Extração dos valores da linha atual
    p_grid_buy = row['Rede_Importacao_kW']
    p_grid_sell = row['Rede_Exportacao_kW']
    
    # Potências dos EVs
    p_ch_ev = row['VE_Carga_kW']
    p_dis_ev = row['VE_Descarga_kW']
    
    # Potências do BESS (assume 0 se a coluna ainda não existir no fluxos_df)
    p_ch_bat = row.get('BESS_Carga_kW', 0)
    p_dis_bat = row.get('BESS_Descarga_kW', 0)
    
    # Carga total da instalação
    p_load = row['Instalacao_PL_kW']
    
    # Totais de carga e descarga
    p_ch_total = p_ch_bat + p_ch_ev
    p_dis_total = p_dis_bat + p_dis_ev
    
    #(sem VC)
    
    # PS (Peak Shaving): BESS ou EV a descarregar para prevenir sobrecarga
    if p_dis_total > tol and p_load > p_grid_max:
        return "PS"
        
    # ARB (Charge): BESS ou EV a carregar enquanto compra à rede
    elif p_ch_total > tol and p_grid_buy > tol:
        return "ARB (Charge)"
        
    # ARB (Discharge): BESS ou EV a descarregar para vender à rede
    elif p_dis_total > tol and p_grid_sell > tol:
        return "ARB (Discharge)"
        
    # SC (Charge): Armazenar excesso de PV no BESS ou EV (sem comprar à rede)
    elif p_ch_total > tol and p_grid_buy <= tol:
        return "SC (Charge)"
        
    # SC (Discharge): Apenas BESS 
    elif p_dis_bat > tol and p_grid_sell <= tol:
        return "SC (Discharge)"
        
    # IDLE: Qualquer outra situação
    else:
        return "IDLE"

# =============================================================================
# APLICAR AO DATAFRAME
# =============================================================================

# Define a potência máxima contratada (P_grid^max) para a instalação
P_GRID_MAX = 6.9 

# Aplicar a função passando a linha do dataframe e o limite da rede
fluxos_df['Modo_Operacao'] = fluxos_df.apply(lambda row: definir_modo_operacao_equacoes(row, P_GRID_MAX), axis=1)

fluxos_df.to_excel(folder + '/Gestao_Energia_e_Veiculos_Final.xlsx', engine='openpyxl')

print("\nFicheiro gerado com Sucesso!")