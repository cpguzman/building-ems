import time
import os
import glob
import pandas as pd
import logging
import math

# Configure logging to display real-time simulation updates
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_value_from_df(df, row_id, col_val, default=0.0):
    """ 
    Helper function to safely extract values from pandas DataFrames (e.g., Pyomo outputs).
    Prevents KeyError crashes if a specific index or column is missing.
    """
    if df is None: return default
    col_str = str(col_val)
    if col_str not in df.columns: return default
    for idx in df.index:
        if str(idx) == str(row_id):
            return float(df.loc[idx, col_str])
    return default

class BessRealTimeController:
    """
    Layer 3: Real-Time Controller.
    Responsible for executing the power management strategy by translating 
    Operational Modes (from Layer 2) into physical power setpoints, 
    compensating for stochastic deviations while ensuring physical constraints.
    """
    def __init__(self, modes_path, cp_path, pv_path, pl_path, bess_path, pbess_ch_path, pbess_dis_path, 
                 evs_path, alpha_path, pev_ch_path, pev_dis_path, p_grid_max):
        
        # p_grid_max is a LIST containing the power limits for each of the 3 phases (kW)
        self.P_GRID_MAX = p_grid_max

        # 1. Load Operational Modes (Evaluated by Layer 2 - Strategy Translator)
        try:
            self.operation_modes = pd.read_csv(modes_path, index_col=0)
            logger.info("Operational modes loaded successfully.")
        except FileNotFoundError:
            self.operation_modes = None
            
        # 2. Load 3-Phase Measurements (PV Generation and Load)
        # header=None prevents Pandas from consuming Phase 1 data as the column header
        try:
            self.pv_data = pd.read_csv(pv_path, header=None)
            self.pl_data = pd.read_csv(pl_path, header=None)
        except Exception as e:
            logger.error(f"Error loading PV/PL data: {e}")
            self.pv_data, self.pl_data = None, None

        # Load Optimizer's day-ahead scheduled setpoints for the BESS
        try:
            self.df_planned_bess_ch = pd.read_csv(pbess_ch_path, index_col=0)
            self.df_planned_bess_dis = pd.read_csv(pbess_dis_path, index_col=0)
        except: 
            self.df_planned_bess_ch, self.df_planned_bess_dis = None, None

        # 3. Configure BESS Hardware Limits and Initial State
        try:
            bess_df = pd.read_csv(bess_path)
            self.BESS_MAX_CH = bess_df['Pmax_charge_rate'].iloc[0] / 1000.0
            self.BESS_MAX_DIS = bess_df['Pmax_discharge_rate'].iloc[0] / 1000.0
            self.BESS_CAPACITY = bess_df['Emax'].iloc[0] / 1000.0
            self.current_soc = bess_df['initial_soc'].iloc[0]
            self.bess_eff = bess_df['eff'].iloc[0]
            logger.info(f"BESS Configured: {self.BESS_CAPACITY} kWh")
        except Exception as e:
            logger.error(f"BESS Error: {e}")

        # 4. Configure EVs Data and State Trackers
        self.ev_states = {}
        self.ev_phases = {} # Maps EV ID -> Phase Index (0, 1, 2)
        
        try:
            evs_df = pd.read_csv(evs_path)
            evs_df.columns = evs_df.columns.str.strip()
            
            # --- Dynamic Mapping: EV -> Charging Point (CP) -> Phase ---
            try:
                cp_df = pd.read_csv(cp_path)
                cp_df.columns = cp_df.columns.str.strip()
                
                # Create a dictionary mapping cp_id to Python Phase Index (0, 1, 2)
                cp_to_fase = {}
                for _, row in cp_df.iterrows():
                    # Subtract 1 because CSV phases are 1, 2, 3 and Python indices are 0, 1, 2
                    cp_to_fase[str(int(row['cp_id']))] = int(row['Fase']) - 1 
            except Exception as e:
                logger.error(f"Error loading cp_inputs.csv: {e}")
                cp_to_fase = {}

            if 'ev_id' in evs_df.columns:
                evs_df.set_index('ev_id', inplace=True)
            elif evs_df.columns[0].lower() in ['id', 'ev', 'nome', 'name', 'veiculo']:
                evs_df.set_index(evs_df.columns[0], inplace=True)
            
            for idx, row in evs_df.iterrows():
                emax_kwh = row['EEVmax'] / 1000.0
                esoc_kwh = row['Esoc'] / 1000.0
                soc_inicial = esoc_kwh / emax_kwh if emax_kwh > 0 else 0.0
                
                self.ev_states[str(idx)] = {
                    'Pmax_ch': row['PchmaxEV'] / 1000.0,
                    'Pmax_dis': row['PdchmaxEV'] / 1000.0,
                    'Emax': emax_kwh,
                    'soc': soc_inicial,
                    'eff': row['evcheff'],
                    # Safely read 'ev target' column. Defaults to 0.90 if missing.
                    'target_soc': float(row.get('ev target', 0.90))
                }
                
                # Link the EV to its physical grid phase
                cp_conectado = str(int(row.get('cpconnected', 1)))
                self.ev_phases[str(idx)] = cp_to_fase.get(cp_conectado, 0) # Defaults to Phase 1
                
            logger.info(f"EVs Configured: {len(self.ev_states)} vehicle(s). Phase Mapping: {self.ev_phases}")
        except Exception as e:
            logger.error(f"Error configuring EVs: {e}")

        # --- Load EV Connection Matrix (Alpha array) ---
        try: 
            self.alpha_data = pd.read_csv(alpha_path)
            if 'Unnamed: 0' in self.alpha_data.columns:
                self.alpha_data.set_index('Unnamed: 0', inplace=True)
        except: 
            self.alpha_data = None

        # Load Optimizer's scheduled setpoints for EVs
        try:
            self.df_planned_ev_ch = pd.read_csv(pev_ch_path, index_col=0)
            self.df_planned_ev_dis = pd.read_csv(pev_dis_path, index_col=0)
        except: 
            self.df_planned_ev_ch, self.df_planned_ev_dis = None, None

    def get_current_mode(self, current_hour):
        """ Retrieves the semantic Operational Mode for the current time step. """
        if self.operation_modes is not None and current_hour in self.operation_modes.index:
            return self.operation_modes.loc[current_hour, 'Modo_Operacao']
        return "IDLE"

    def get_measurements_for_hour(self, current_hour):
        """ 
        Simulates Real-Time sensor readings by extracting 3-Phase PV and Load data.
        Skips row 0 (time headers) and reads rows 1, 2, and 3 (Phases 1, 2, 3).
        """
        col_idx = current_hour - 1 
        
        pv_fases = [0.0, 0.0, 0.0]
        pl_fases = [0.0, 0.0, 0.0]
        
        if self.pv_data is not None and col_idx < len(self.pv_data.columns):
            # iloc[1:4, col_idx] extracts rows 1 to 3
            raw_pv = self.pv_data.iloc[1:4, col_idx].values / 1000.0
            for i in range(len(raw_pv)):
                pv_fases[i] = float(raw_pv[i])

        if self.pl_data is not None and col_idx < len(self.pl_data.columns):
            raw_pl = self.pl_data.iloc[1:4, col_idx].values / 1000.0
            for i in range(len(raw_pl)):
                pl_fases[i] = float(raw_pl[i])
            
        return pv_fases, pl_fases
    
    def get_optimizer_setpoints(self, current_hour):
        """ Fetches Pyomo's day-ahead planned Net Power Setpoints (Charge - Discharge). """
        bess_ch = get_value_from_df(self.df_planned_bess_ch, self.df_planned_bess_ch.index[0] if self.df_planned_bess_ch is not None else 0, current_hour) / 1000.0
        bess_dis = get_value_from_df(self.df_planned_bess_dis, self.df_planned_bess_dis.index[0] if self.df_planned_bess_dis is not None else 0, current_hour) / 1000.0
        bess_planned = bess_ch - bess_dis

        evs_planned = {}
        alphas = {}
        for ev_id in self.ev_states.keys():
            ev_ch = get_value_from_df(self.df_planned_ev_ch, ev_id, current_hour) / 1000.0
            ev_dis = get_value_from_df(self.df_planned_ev_dis, ev_id, current_hour) / 1000.0
            evs_planned[ev_id] = ev_ch - ev_dis
            
            a_val = get_value_from_df(self.alpha_data, ev_id, current_hour, default=None)
            if a_val is None:
                a_val = get_value_from_df(self.alpha_data, self.alpha_data.index[0] if self.alpha_data is not None else 0, current_hour, default=0)
            
            alphas[ev_id] = int(a_val)
            
        return bess_planned, evs_planned, alphas
    
    def get_hours_until_FINAL_departure(self, ev_id, current_hour):
        """
        Calcula as horas restantes úteis até o utilizador se ir embora.
        Calculates the remaining useful hours until the EV's final disconnection of the day.
        Includes a safe fallback logic to handle CSV formatting issues.
        """
        if self.alpha_data is None:
            return 24 - current_hour
            
        # Helper function for safe alpha reading
        def get_alpha_seguro(h):
            a_val = get_value_from_df(self.alpha_data, ev_id, h, default=None)
            if a_val is None:
                a_val = get_value_from_df(self.alpha_data, self.alpha_data.index[0] if self.alpha_data is not None else 0, h, default=0)
            return a_val

        ultima_hora_ligado = -1
        # Scan backwards to find the final connection hour
        for h in range(24, current_hour - 1, -1): 
            if get_alpha_seguro(h) == 1:
                ultima_hora_ligado = h
                break
                
        if ultima_hora_ligado == -1 or current_hour > ultima_hora_ligado:
            return -1

        horas_uteis_restantes = 0
        for h in range(current_hour, ultima_hora_ligado + 1):
            if get_alpha_seguro(h) == 1:
                horas_uteis_restantes += 1
                
        return horas_uteis_restantes

    def calculate_setpoints(self, current_hour, current_mode, pv_fases, pl_fases, bess_planned, evs_planned, alphas):
        """
        Core Real-Time Control Logic.
        Calculates actionable setpoints adhering to physical limits and user constraints.
        """
        if not pv_fases or not pl_fases:
            logger.warning("Invalid sensors. System in STANDBY.")
            return 0.0, {ev_id: 0.0 for ev_id in self.ev_states.keys()}, [0.0, 0.0, 0.0]

        # Calculate uncontrolled net load per phase (Load - PV)
        net_fases = [pl_fases[i] - pv_fases[i] for i in range(3)]
        folga_fases = [self.P_GRID_MAX[i] - net_fases[i] for i in range(3)] # Grid power margin
        current_net_load_global = sum(net_fases)
        
        ev_setpoints = {ev_id: 0.0 for ev_id in self.ev_states.keys()}
        bess_setpoint_global = 0.0
        
        # Calculate instantaneous BESS limits based on current SoC
        max_ch_soc_bess = ((1.0 - self.current_soc) * self.BESS_CAPACITY) / self.bess_eff
        act_max_ch_bess = min(self.BESS_MAX_CH, max_ch_soc_bess)
        max_dis_soc_bess = ((self.current_soc - 0.05) * self.BESS_CAPACITY) * self.bess_eff
        act_max_dis_bess = min(self.BESS_MAX_DIS, max_dis_soc_bess)

        # ====================================================================
        # PRIORITY ASSESSMENT - DYNAMIC EMERGENCY TRIGGER
        # ====================================================================
        emergencia_ativa = False
        potencia_emergencia_necessaria = [0.0, 0.0, 0.0] 
        ev_em_emergencia = {ev_id: False for ev_id in self.ev_states.keys()} # Tracking EVs requiring urgent charge
        
        for ev_id, state in self.ev_states.items():
            if alphas.get(ev_id, 0) == 0: continue 
            
            horas_uteis_restantes = self.get_hours_until_FINAL_departure(ev_id, current_hour)
            soc_objetivo = state.get('target_soc', 0.90)  
            
            # Check if EV has not reached the Target SoC
            if 0 < horas_uteis_restantes and state['soc'] < (soc_objetivo - 0.01): 
                
                # 1. How much energy is missing?
                energia_em_falta = ((soc_objetivo - state['soc']) * state['Emax']) / state['eff']
                
                # 2. What is the realistic maximum charging power? (Limited by Charger or Phase Grid Limit)
                fase_do_ev = self.ev_phases.get(ev_id, 0)
                pot_max_realista = min(state['Pmax_ch'], self.P_GRID_MAX[fase_do_ev])
                
                # 3. How many hours at full power are needed to reach the target?
                horas_minimas_necessarias = energia_em_falta / pot_max_realista
                
                # SMART TRIGGER: Enter Emergency Mode only if the remaining time is critically low
                # (Remaining hours <= Required hours + 1 hour safety margin)
                margem_seguranca = 1.0 
                
                if horas_uteis_restantes <= (horas_minimas_necessarias + margem_seguranca): 
                    emergencia_ativa = True
                    ev_em_emergencia[ev_id] = True
                    
                    # SMOOTHING ALGORITHM: 
                    # Divides the required energy over the remaining hours to prevent demand spikes
                    pot_necessaria_suavizada = energia_em_falta / horas_uteis_restantes
                    pot_necessaria = min(pot_necessaria_suavizada, state['Pmax_ch'])
                    
                    potencia_emergencia_necessaria[fase_do_ev] += pot_necessaria
                    
                    logger.warning(f" EV URGENCY {ev_id}: SOC={state['soc']*100:.1f}%. Smoothing charge to {pot_necessaria:.2f} kW for this hour.")
        
        # ====================================================================
        # ROUTE A: EMERGENCY (Critical Urgency Override - Prioritize EV Mobility)
        # ====================================================================
        if emergencia_ativa:
            logger.info("-> OVERRIDE ACTIVE: Ignoring economic Optimizer due to lack of time.")
            
            bess_sp = 0.0
            # Calculate if the EV's urgent charging will overload the grid phase
            pior_sobrecarga = max([net_fases[i] + potencia_emergencia_necessaria[i] - self.P_GRID_MAX[i] for i in range(3)])
            
            # If an overload is predicted, force BESS to discharge to protect the circuit breaker
            if pior_sobrecarga > 0 and act_max_dis_bess > 0:
                bess_sp = -min(pior_sobrecarga * 3.0, act_max_dis_bess)
                logger.info(f"-> BESS forced to discharge {abs(bess_sp):.2f} kW to protect the grid.")
                
            bess_setpoint_global = bess_sp
            bess_por_fase = bess_setpoint_global / 3.0
            
            # Update physical grid states with BESS intervention
            for i in range(3):
                net_fases[i] += bess_por_fase
                folga_fases[i] = self.P_GRID_MAX[i] - net_fases[i]

            # Execute Emergency Charging for Critical EVs
            for ev_id, state in self.ev_states.items():
                if alphas.get(ev_id, 0) == 0: continue
                
                fase_do_ev = self.ev_phases.get(ev_id, 0)
                margem_fase_ev = max(0.0, folga_fases[fase_do_ev])
                
                ev_max_ch_soc = ((1.0 - state['soc']) * state['Emax']) / state['eff']
                act_max_ch_ev = min(state['Pmax_ch'], ev_max_ch_soc)
                
                if ev_em_emergencia.get(ev_id, False):
                    energia_em_falta = ((state.get('target_soc', 0.90) - state['soc']) * state['Emax']) / state['eff']
                    
                    # Apply proportional smoothing in execution
                    horas_restantes = self.get_hours_until_FINAL_departure(ev_id, current_hour)
                    pot_alvo_suavizada = energia_em_falta / horas_restantes if horas_restantes > 0 else energia_em_falta
                    
                    pot_alvo = min(pot_alvo_suavizada, act_max_ch_ev)
                    ev_sp = min(pot_alvo, margem_fase_ev)
                else:
                    ev_sp = 0.0
                
                # Cleanup floating point noise
                if abs(ev_sp) < 0.1: ev_sp = 0.0
                if ev_sp > 0: ev_sp = math.floor(ev_sp * 10) / 10.0
                elif ev_sp < 0: ev_sp = math.ceil(ev_sp * 10) / 10.0
                
                ev_setpoints[ev_id] = ev_sp
                net_fases[fase_do_ev] += ev_sp
                folga_fases[fase_do_ev] -= ev_sp

        # ====================================================================
        # ROUTE B: NORMAL OPERATION (Follow Semantic Modes from Layer 2)
        # ====================================================================
        else:
            # Constrain BESS plan with its real-time physical limitations
            if bess_planned is not None:
                if bess_planned > 0.001:
                    act_max_ch_bess = min(act_max_ch_bess, bess_planned)
                    act_max_dis_bess = 0.0
                elif bess_planned < -0.001:
                    act_max_dis_bess = min(act_max_dis_bess, abs(bess_planned))
                    act_max_ch_bess = 0.0
                else:
                    act_max_ch_bess, act_max_dis_bess = 0.0, 0.0

            fase_critica = min(folga_fases)
            grid_margin_charge_global = max(0.0, fase_critica * 3.0)

            # Translation Layer Semantic Execution (BESS)
            if current_mode == "PS": # Peak Shaving
                sobrecargas = [max(0.0, net_fases[i] - self.P_GRID_MAX[i]) for i in range(3)]
                pior_sobrecarga = max(sobrecargas)
                if pior_sobrecarga > 0:
                    bess_setpoint_global = -min(pior_sobrecarga * 3.0, act_max_dis_bess)
            elif current_mode == "SC (Charge)" and current_net_load_global < 0: # Self-Consumption (Store PV)
                bess_setpoint_global = min(abs(current_net_load_global), act_max_ch_bess, grid_margin_charge_global)
            elif current_mode in ["SC (Discharge)", "V2H"] and current_net_load_global > 0: # Discharge to supply home
                bess_setpoint_global = -min(current_net_load_global, act_max_dis_bess)
            elif current_mode == "ARB (Charge)": # Arbitrage: Grid Energy is cheap
                bess_setpoint_global = min(act_max_ch_bess, grid_margin_charge_global)
            elif current_mode == "ARB (Discharge)": # Arbitrage: Sell to Grid
                bess_setpoint_global = -act_max_dis_bess

            if abs(bess_setpoint_global) < 0.1: bess_setpoint_global = 0.0
            if bess_setpoint_global > 0: bess_setpoint_global = math.floor(bess_setpoint_global * 10) / 10.0
            elif bess_setpoint_global < 0: bess_setpoint_global = math.ceil(bess_setpoint_global * 10) / 10.0

            bess_por_fase = bess_setpoint_global / 3.0
            for i in range(3):
                net_fases[i] += bess_por_fase
                folga_fases[i] = self.P_GRID_MAX[i] - net_fases[i]

            # Translation Layer Semantic Execution (EVs)
            for ev_id, state in self.ev_states.items():
                if alphas.get(ev_id, 0) == 0: continue
                
                fase_do_ev = self.ev_phases.get(ev_id, 0)
                margem_fase_ev = max(0.0, folga_fases[fase_do_ev])
                planned = evs_planned.get(ev_id, 0.0)
                
                # ==========================================================
                # ROUTE B: SMART CEILING LIMITER (Anti-Overshoot)
                # ==========================================================
                teto_soc = 1.0 # Default battery natural limit is 100%
                soc_objetivo_utilizador = state['target_soc'] # Fetches user target dynamically
                
                # If approaching departure time, prevent Optimizer from overcharging past the Target SoC
                horas_rest = self.get_hours_until_FINAL_departure(ev_id, current_hour)
                if 0 < horas_rest <= 4:
                    teto_soc = soc_objetivo_utilizador 
                
                # Calculate how much energy can be added before hitting the ceiling
                ev_max_ch_soc = (max(0.0, teto_soc - state['soc']) * state['Emax']) / state['eff']
                act_max_ch_ev = min(state['Pmax_ch'], ev_max_ch_soc)
                
                ev_max_dis_soc = ((state['soc'] - 0.05) * state['Emax']) * state['eff']
                act_max_dis_ev = min(state['Pmax_dis'], ev_max_dis_soc)
                # ==========================================================

                ev_sp = 0.0 # Default state: IDLE

                if current_mode == "PS":
                    if net_fases[fase_do_ev] > self.P_GRID_MAX[fase_do_ev] and act_max_dis_ev > 0:
                        ev_sp = -min(net_fases[fase_do_ev] - self.P_GRID_MAX[fase_do_ev], act_max_dis_ev)
                    elif planned > 0:
                        ev_sp = min(planned, act_max_ch_ev, margem_fase_ev)
                    elif planned < 0:
                        ev_sp = -min(abs(planned), act_max_dis_ev)
                        
                elif current_mode == "SC (Charge)":
                    if net_fases[fase_do_ev] < 0: # PV surplus available
                        ev_sp = min(abs(net_fases[fase_do_ev]), act_max_ch_ev)
                        if planned > ev_sp:       # If Optimizer requested more than solar, bound it
                            ev_sp = min(planned, act_max_ch_ev, margem_fase_ev)
                    else:                         # No solar surplus, strictly follow plan bounds
                        if planned > 0: ev_sp = min(planned, act_max_ch_ev, margem_fase_ev)
                        elif planned < 0: ev_sp = -min(abs(planned), act_max_dis_ev)
                        
                elif current_mode == "SC (Discharge)":
                    if planned > 0: ev_sp = min(planned, act_max_ch_ev, margem_fase_ev)
                    elif planned < 0: ev_sp = -min(abs(planned), act_max_dis_ev)
                        
                elif current_mode == "V2H":
                    if net_fases[fase_do_ev] > 0 and act_max_dis_ev > 0:
                        ev_sp = -min(net_fases[fase_do_ev], act_max_dis_ev)
                    elif planned > 0: ev_sp = min(planned, act_max_ch_ev, margem_fase_ev)
                    elif planned < 0: ev_sp = -min(abs(planned), act_max_dis_ev)
                        
                elif current_mode in ["ARB (Charge)", "ARB (Discharge)"]:
                    # In Arbitrage, the EV blindly follows the Optimizer's market-driven plan
                    if planned > 0:
                        ev_sp = min(planned, act_max_ch_ev, margem_fase_ev)
                    elif planned < 0:
                        ev_sp = -min(abs(planned), act_max_dis_ev)

                # Rounding and noise cleanup
                if abs(ev_sp) < 0.1: ev_sp = 0.0
                if ev_sp > 0: ev_sp = math.floor(ev_sp * 10) / 10.0
                elif ev_sp < 0: ev_sp = math.ceil(ev_sp * 10) / 10.0

                ev_setpoints[ev_id] = ev_sp
                net_fases[fase_do_ev] += ev_sp
                folga_fases[fase_do_ev] -= ev_sp

        return bess_setpoint_global, ev_setpoints, net_fases

    def publish_and_update_soc(self, bess_setpoint, ev_setpoints):
        """ 
        Updates the internal State of Charge (SoC) for all batteries 
        based on the validated and dispatched power setpoints.
        """
        if bess_setpoint > 0:
            transfer = (bess_setpoint * 1.0) * self.bess_eff 
            logger.info(f"BESS: CHARGING at {bess_setpoint} kW")
        elif bess_setpoint < 0:
            transfer = (bess_setpoint * 1.0) / self.bess_eff 
            logger.info(f"BESS: DISCHARGING at {abs(bess_setpoint)} kW")
        else:
            transfer = 0.0
            logger.info("BESS: STANDBY (0 kW)")

        self.current_soc = max(0.0, min(1.0, ((self.current_soc * self.BESS_CAPACITY) + transfer) / self.BESS_CAPACITY))
        logger.info(f"-> BESS SOC: {self.current_soc*100:.1f} %")

        for ev_id, sp in ev_setpoints.items():
            state = self.ev_states[ev_id]
            if sp > 0:
                transfer = (sp * 1.0) * state['eff']
                logger.info(f"EV {ev_id}: CHARGING at {sp} kW")
            elif sp < 0:
                transfer = (sp * 1.0) / state['eff']
                logger.info(f"EV {ev_id}: DISCHARGING at {abs(sp)} kW")
            else:
                transfer = 0.0
            
            nova_soc = max(0.0, min(1.0, ((state['soc'] * state['Emax']) + transfer) / state['Emax']))
            self.ev_states[ev_id]['soc'] = nova_soc
            if sp != 0:
                logger.info(f"-> EV {ev_id} SOC: {nova_soc*100:.1f} %")


def main():
    """
    Main Execution Block.
    Provides a interface to select the simulation scenario, runs the chronological 
    simulation loop, exports the results to CSV, and generates plots.
    """
    pastas = glob.glob('RESULTS_*')
    if not pastas:
        print("Nenhuma pasta 'RESULTS_*' encontrada.")
        return
        
    print("\n" + "="*50 + "\n Pastas Disponíveis:\n" + "="*50)
    for i, pasta in enumerate(pastas): print(f"[{i}] - {pasta}")
    
    escolha = input("\nNúmero da pasta para simular: ")
    pasta_escolhida = pastas[int(escolha)]
        
    pasta_medicoes = 'measurements'
    
    # --- READ POWER LIMITS PER PHASE (Grid Constraints) ---
    try:
        df_css = pd.read_csv(os.path.join(pasta_medicoes, 'css_power.csv'), header=None)
        # Assumes values are in the first column, extracts the last 3 rows (3 phases)
        valores_limite = df_css[0].tail(3).values 
        limites_fases_kw = [v / 1000.0 for v in valores_limite]
        logger.info(f"Phase Limits Loaded: {limites_fases_kw} kW")
    except Exception as e:
        limites_fases_kw = [6.9, 6.9, 6.9] # Safety default if file is missing
        logger.warning(f"Error reading css_power.csv: {e}. Using default limit of 6.9 kW per phase.")
    
    controller = BessRealTimeController(
        modes_path=os.path.join(pasta_escolhida, 'Modos_Operacao_Analisados.csv'),
        pv_path=os.path.join(pasta_medicoes, 'pv.csv'),
        pl_path=os.path.join(pasta_medicoes, 'pl.csv'),
        bess_path=os.path.join(pasta_medicoes, 'bess_inputs.csv'),
        pbess_ch_path=os.path.join(pasta_escolhida, 'PBess.csv'),
        pbess_dis_path=os.path.join(pasta_escolhida, 'PBessdc.csv'),
        evs_path=os.path.join(pasta_medicoes, 'evs_inputs.csv'),
        cp_path=os.path.join(pasta_medicoes, 'cp_inputs.csv'),
        alpha_path=os.path.join(pasta_medicoes, 'alpha.csv'),
        pev_ch_path=os.path.join(pasta_escolhida, 'PEV.csv'),
        pev_dis_path=os.path.join(pasta_escolhida, 'PEVdc.csv'),
        p_grid_max=limites_fases_kw
    )

    print("\n" + "="*60)
    logger.info(f"STARTING RTO SIMULATION - Global Limit: {sum(limites_fases_kw):.1f} kW")
    print("="*60 + "\n")
    
    historico_resultados = []
    
    for hora_atual in range(1, 25):
        logger.info(f"--- [ HOUR {hora_atual:02d}:00 ] ---")
        
        current_mode = controller.get_current_mode(hora_atual)
        
        pv_fases, pl_fases = controller.get_measurements_for_hour(hora_atual)
        bess_planned, evs_planned, alphas = controller.get_optimizer_setpoints(hora_atual)
        
        logger.info(f"Global Grid -> PV: {sum(pv_fases):.2f} kW | Load: {sum(pl_fases):.2f} kW | Mode: {current_mode}")
        logger.info(f"Optimizer Plan -> BESS: {bess_planned:.2f} kW")
        
        # Calculate optimal setpoints ensuring grid limits and physical capabilities
        bess_sp, ev_sps, net_fases_final = controller.calculate_setpoints(hora_atual, current_mode, pv_fases, pl_fases, bess_planned, evs_planned, alphas)
        
        controller.publish_and_update_soc(bess_sp, ev_sps)
        
        # Calculate Import (Values > 0) and Export (Values < 0) per phase
        import_fases = [max(0.0, net) for net in net_fases_final]
        export_fases = [abs(min(0.0, net)) for net in net_fases_final]
        
        row_data = {
            'Hora': hora_atual,
            'Modo_Operacao': current_mode,
            
            # --- PV GENERATION PER PHASE ---
            'PV_L1_kW': round(pv_fases[0], 3),
            'PV_L2_kW': round(pv_fases[1], 3),
            'PV_L3_kW': round(pv_fases[2], 3),
            'PV_Global_kW': round(sum(pv_fases), 3),
            
            # --- BASE LOAD PER PHASE ---
            'Load_L1_kW': round(pl_fases[0], 3),
            'Load_L2_kW': round(pl_fases[1], 3),
            'Load_L3_kW': round(pl_fases[2], 3),
            'Load_Global_kW': round(sum(pl_fases), 3),
            
            # --- PUBLIC GRID RELATION (IMPORT) ---
            'Grid_Import_L1_kW': round(import_fases[0], 3),
            'Grid_Import_L2_kW': round(import_fases[1], 3),
            'Grid_Import_L3_kW': round(import_fases[2], 3),
            'Grid_Import_Global_kW': round(sum(import_fases), 3),
            
            # --- PUBLIC GRID RELATION (EXPORT) ---
            'Grid_Export_L1_kW': round(export_fases[0], 3),
            'Grid_Export_L2_kW': round(export_fases[1], 3),
            'Grid_Export_L3_kW': round(export_fases[2], 3),
            'Grid_Export_Global_kW': round(sum(export_fases), 3),

            # --- BESS TRACKING ---
            'Opt_BESS_kW': round(bess_planned, 3),
            'Setpoint_BESS_Global_kW': bess_sp,
            'SOC_BESS_%': round(controller.current_soc * 100, 2),
            'Energy_BESS_kWh': round(controller.current_soc * controller.BESS_CAPACITY, 3)
        }
        
        # --- EV TRACKING ---
        for ev_id, ev_sp in ev_sps.items():
            fase_do_ev = controller.ev_phases.get(ev_id, 0) + 1 # +1 to display L1, L2 or L3
            row_data[f'Fase_EV{ev_id}'] = f"L{fase_do_ev}"
            row_data[f'Alpha_EV{ev_id}'] = alphas[ev_id]
            row_data[f'Opt_EV{ev_id}_kW'] = round(evs_planned[ev_id], 3)
            row_data[f'Setpoint_EV{ev_id}_kW'] = ev_sp
            row_data[f'SOC_EV{ev_id}_%'] = round(controller.ev_states[ev_id]['soc'] * 100, 2)
            
            state = controller.ev_states[ev_id] 
            row_data[f'Energy_EV{ev_id}_kWh'] = round(state['soc'] * state['Emax'], 3)
            
        historico_resultados.append(row_data)
        
        print("\n")
        time.sleep(0.5)

    df_resultados = pd.DataFrame(historico_resultados)
    caminho_output = os.path.join(pasta_escolhida, 'Resultados_Simulacao_RTO_EVS.csv')
    df_resultados.to_csv(caminho_output, index=False)
    
    # =============================================================================
    # PLOT GENERATION (Power Balance Visualization)
    # =============================================================================
    import matplotlib.pyplot as plt
    import numpy as np
    
    print("\nA gerar o gráfico final de balanço de potência RTO...")

    # Create a figure with shared scale (3 subplots, 1 for each grid phase)
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True, sharey=True)
    fig.suptitle('Power Balance (RTO) per Phase', fontsize=18, fontweight='bold')

    # X-axis represents hours 1 through 24
    time_steps = df_resultados['Hora'].tolist()
    
    for f in range(1, 4):  # Phases 1, 2, 3
        ax = axes[f-1]
        
        # --- 1. Data Extraction & Conversion (kW to W) ---
        # Base Load and PV Generation
        pl_vals = (df_resultados[f'Load_L{f}_kW'] * 1000).tolist()
        pv_vals = (-df_resultados[f'PV_L{f}_kW'] * 1000).tolist()  # Drawn downwards (negative)
        
        # Net Grid (Import = Positive, Export = Negative)
        net_grid = ((df_resultados[f'Grid_Import_L{f}_kW'] - df_resultados[f'Grid_Export_L{f}_kW']) * 1000).tolist()
        
        # BESS (Symmetrical 3-phase, divide global by 3. Charge = Negative, Discharge = Positive)
        net_bess = (-df_resultados['Setpoint_BESS_Global_kW'] / 3.0 * 1000).tolist()
        
        # Net EVs (Aggregates only the EVs physically connected to this phase)
        net_ev = np.zeros(24)
        for ev_id in controller.ev_states.keys():
            if controller.ev_phases.get(ev_id, 0) + 1 == f:
                sp_kw = df_resultados[f'Setpoint_EV{ev_id}_kW']
                # Subtracted because EV Charging (Positive Setpoint) = Grid Demand (Negative Plot line)
                net_ev -= (sp_kw * 1000)
        net_ev = net_ev.tolist()

        # Contracted Limits (Extracted from css_power.csv limits)
        limite_fase_w = limites_fases_kw[f-1] * 1000
        pt_vals_pos = [limite_fase_w] * 24
        pt_vals_neg = [-limite_fase_w] * 24
        
        # --- 2. DRAWING THE LINES ---
        ax.plot(time_steps, pl_vals, label='Base Load', color='black', linewidth=1, alpha=0.6)
        ax.fill_between(time_steps, 0, pv_vals, label='Solar Production', color='orange', alpha=0.3)
        
        # Grid Capacity Limits (Dashed Red Lines)
        ax.plot(time_steps, pt_vals_pos, label='Grid Limit (Import/Export)', color='red', linestyle='--', linewidth=1.5)
        ax.plot(time_steps, pt_vals_neg, color='red', linestyle='--', linewidth=1.5)
        
        ax.plot(time_steps, net_grid, label='Net Grid', color='dodgerblue', linewidth=2)
        ax.plot(time_steps, net_bess, label='Net BESS', color='purple', linewidth=2, marker='o', markersize=3)
        ax.plot(time_steps, net_ev, label='Net EVs', color='crimson', linewidth=2, marker='s', markersize=3)
        
        # --- 3. STYLING & FORMATTING ---
        ax.set_title(f'Phase {f}', fontsize=14, fontweight='bold')
        ax.set_ylabel('Power (W)')
        ax.grid(True, linestyle=':', alpha=0.6)
        ax.axhline(0, color='black', linewidth=1)
        
        # X-axis limits and ticks (Hourly intervals)
        ax.set_xlim(1, 24)
        ax.set_xticks(time_steps)
        
        ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0., fontsize='small')

    axes[-1].set_xlabel('Time (Hours)')
    plt.tight_layout(rect=[0, 0, 0.85, 0.96])

    # Save output plot
    plot_path = os.path.join(pasta_escolhida, 'Grafico_Final_Balanço_RTO.png')
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close() 
    logger.info(f"Plot saved at: {plot_path}")
    
    print("="*60)
    logger.info(f"Simulação concluída. Ficheiro guardado em: {caminho_output}")
    print("="*60 + "\n")

if __name__ == '__main__':
    main()
