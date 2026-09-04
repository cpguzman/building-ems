# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 10:57:18 2026

@author: rafae
"""

import time
import os
import glob
import pandas as pd
import logging
import math
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch
from matplotlib.lines import Line2D

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_value_from_df(df, row_id, col_val, default=0.0):
    """
    Retrieves a numeric value from a DataFrame given a row index and a column name.
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
    Layer 3: Real-Time Controller 
    Attempts to blindly execute the optimizer's pre-calculated setpoints. 
    """
    def __init__(self, modes_path, pv_path, pl_path, bess_path, pbess_ch_path, pbess_dis_path, cp_path,
                 evs_path, alpha_path, pev_ch_path, pev_dis_path, p_grid_max, prices_path):
        """
        Initializes the controller by loading measurements, prices, hardware limits, 
        and specifically the day-ahead planned setpoints for the BESS and EVs.
        """
        self.P_GRID_MAX = p_grid_max 
        
        try:
            if prices_path and os.path.exists(prices_path):
                self.prices_data = pd.read_csv(prices_path)
            else:
                self.prices_data = None
        except Exception as e:
            logger.error(f"Erro ao ler preços: {e}")
            self.prices_data = None

        try:
            self.operation_modes = pd.read_csv(modes_path, index_col=0)
        except:
            self.operation_modes = None
            
        try:
            self.pv_data = pd.read_csv(pv_path, header=None)
            self.pl_data = pd.read_csv(pl_path, header=None)
        except:
            self.pv_data, self.pl_data = None, None

        try:
            self.df_planned_bess_ch = pd.read_csv(pbess_ch_path, index_col=0)
            self.df_planned_bess_dis = pd.read_csv(pbess_dis_path, index_col=0)
        except: 
            self.df_planned_bess_ch, self.df_planned_bess_dis = None, None

        try:
            bess_df = pd.read_csv(bess_path)
            self.BESS_MAX_CH = bess_df['Pmax_charge_rate'].iloc[0] / 1000.0
            self.BESS_MAX_DIS = bess_df['Pmax_discharge_rate'].iloc[0] / 1000.0
            self.BESS_CAPACITY = bess_df['Emax'].iloc[0] / 1000.0
            self.current_soc = bess_df['initial_soc'].iloc[0]
            self.bess_eff = bess_df['eff'].iloc[0]
        except Exception as e:
            logger.error(f"BESS Error: {e}")

        # ====================================================================
        # LER A INFRAESTRUTURA DE CARREGAMENTO (Fichas / Pontos de Carregamento)
        # ====================================================================
        cp_types = {}
        cp_mins = {} 
        cp_maxs = {}
        try:
            if cp_path and os.path.exists(cp_path):
                cp_df = pd.read_csv(cp_path)
                cp_df.columns = cp_df.columns.str.strip()
                for idx, row in cp_df.iterrows():
                    cp_id = int(row['cp_id'])
                    cp_type = int(row['Type'])
                    
                    # Tenta ler o Máximo da ficha
                    cp_max = 7.2
                    if 'Pcpmax' in row:
                        cp_max = float(row['Pcpmax']) / 1000.0
                    
                    # Tenta ler o Mínimo da ficha
                    min_ch = 0.0
                    if 'Min Charge (W)' in row:
                        min_ch = float(row['Min Charge (W)']) / 1000.0
                    elif 'Min Charge (kW)' in row:
                        min_ch = float(row['Min Charge (kW)'])
                        
                    cp_types[cp_id] = cp_type
                    cp_mins[cp_id] = min_ch
                    cp_maxs[cp_id] = cp_max
        except Exception as e:
            logger.error(f"Erro ao ler cp_inputs: {e}")

        # ====================================================================
        # LER OS VEÍCULOS E CRUZAR COM OS CARREGADORES
        # ====================================================================
        self.ev_states = {}
        try:
            evs_df = pd.read_csv(evs_path)
            evs_df.columns = evs_df.columns.str.strip()
            
            if 'ev_id' in evs_df.columns:
                evs_df.set_index('ev_id', inplace=True)
            elif evs_df.columns[0].lower() in ['id', 'ev', 'nome', 'name', 'veiculo']:
                evs_df.set_index(evs_df.columns[0], inplace=True)
            
            for idx, row in evs_df.iterrows():
                ev_id_limpo = str(idx).strip()
                emax_kwh = row['EEVmax'] / 1000.0
                esoc_kwh = row['Esoc'] / 1000.0
                soc_inicial = esoc_kwh / emax_kwh if emax_kwh > 0 else 0.0
                
                raw_target = row.get('ev target', 0.90)
                target_soc_limpo = 0.90 if pd.isna(raw_target) else float(raw_target)
                
                # CRUZAMENTO DOS DADOS
                ficha_ligada = int(row.get('cpconnected', 1))
                tipo_da_ficha = cp_types.get(ficha_ligada, 1) 
                is_bin = True if tipo_da_ficha == 2 else False
                
                # ---> LER E CRUZAR POTÊNCIAS MÁXIMAS <---
                ev_pmax_ch = row['PchmaxEV'] / 1000.0
                cp_pmax = cp_maxs.get(ficha_ligada, ev_pmax_ch)
                real_pmax_ch = min(ev_pmax_ch, cp_pmax) 
                
                ev_pmax_dis = row['PdchmaxEV'] / 1000.0
                real_pmax_dis = min(ev_pmax_dis, cp_pmax)
                
                # ---> LER E CRUZAR POTÊNCIAS MÍNIMAS <---
                ev_min_ch = 0.0
                if 'Min Charge (kW)' in row:
                    ev_min_ch = float(row['Min Charge (kW)'])
                elif 'Min Charge (W)' in row:
                    ev_min_ch = float(row['Min Charge (W)']) / 1000.0
                    
                cp_min_charge_kw = cp_mins.get(ficha_ligada, 0.0)
                real_pmin_ch = max(ev_min_ch, cp_min_charge_kw) 
                
                self.ev_states[ev_id_limpo] = {
                    'Pmax_ch': real_pmax_ch,  
                    'Pmax_dis': real_pmax_dis,
                    'Pmin_ch': real_pmin_ch,
                    'Emax': emax_kwh,
                    'soc': soc_inicial,
                    'eff': row.get('evcheff', 0.95),
                    'dch_eff': row.get('evdcheff', 0.95),
                    'target_soc': target_soc_limpo,
                    'is_binary': is_bin 
                }
        except Exception as e:
            logger.error(f"Error configuring EVs: {e}")

        try: 
            self.alpha_data = pd.read_csv(alpha_path)
            if 'Unnamed: 0' in self.alpha_data.columns:
                self.alpha_data.set_index('Unnamed: 0', inplace=True)
        except: 
            self.alpha_data = None

        # Guardamos os dados do Otimizador apenas para Log e comparação no Excel
        try:
            self.df_planned_ev_ch = pd.read_csv(pev_ch_path, index_col=0)
            self.df_planned_ev_dis = pd.read_csv(pev_dis_path, index_col=0)
        except: 
            self.df_planned_ev_ch, self.df_planned_ev_dis = None, None

    def get_current_mode(self, current_hour):
        """Fetches the operational mode (though primarily ignored in Case B execution)."""
        if self.operation_modes is not None and current_hour in self.operation_modes.index:
            return self.operation_modes.loc[current_hour, 'Modo_Operacao']
        return "IDLE"

    def get_measurements_for_hour(self, current_hour):
        """Fetches building load and solar generation in kW for the requested hour."""
        col_idx = current_hour - 1 
        pv_val = 0.0
        pl_val = 0.0
        
        if self.pv_data is not None:
            if self.pv_data.shape[0] >= 24: pv_array = self.pv_data.iloc[:, -1].values
            else: pv_array = self.pv_data.iloc[-1, :].values
            if col_idx < len(pv_array): pv_val = float(pv_array[col_idx]) / 1000.0

        if self.pl_data is not None:
            if self.pl_data.shape[0] >= 24: pl_array = self.pl_data.iloc[:, -1].values
            else: pl_array = self.pl_data.iloc[-1, :].values
            if col_idx < len(pl_array): pl_val = float(pl_array[col_idx]) / 1000.0
            
        return pv_val, pl_val
    
    def get_price_for_hour(self, current_hour):
        """Fetches dynamic grid import and export prices."""
        if self.prices_data is None: 
            return None, None
        col_idx = current_hour - 1
        try:
            # Tenta ler a importação
            if 'import_price' in self.prices_data.columns:
                p_imp = float(self.prices_data['import_price'].iloc[col_idx])
            else:
                p_imp = float(self.prices_data.iloc[col_idx, 1])
                
            # Tenta ler a exportação (venda)
            if 'export_price' in self.prices_data.columns:
                p_exp = float(self.prices_data['export_price'].iloc[col_idx])
            else:
                p_exp = float(self.prices_data.iloc[col_idx, 2])
                
            return p_imp, p_exp
        except:
            return None, None
    
    def get_optimizer_setpoints(self, current_hour):
        """
        Retrieves the exact power setpoints that the day-ahead optimizer calculated.
        In Case B, these setpoints are the primary drivers of execution.
        """
        bess_ch = get_value_from_df(self.df_planned_bess_ch, self.df_planned_bess_ch.index[0] if self.df_planned_bess_ch is not None else 0, current_hour) / 1000.0
        bess_dis = get_value_from_df(self.df_planned_bess_dis, self.df_planned_bess_dis.index[0] if self.df_planned_bess_dis is not None else 0, current_hour) / 1000.0
        bess_planned = bess_ch - bess_dis

        evs_planned_log = {}
        alphas = {}
        
        for i, ev_id in enumerate(self.ev_states.keys()):
            ev_ch = 0.0
            if self.df_planned_ev_ch is not None and str(current_hour) in self.df_planned_ev_ch.columns:
                if i < len(self.df_planned_ev_ch):
                    ev_ch = self.df_planned_ev_ch.iloc[i][str(current_hour)] / 1000.0
                    
            ev_dis = 0.0
            if self.df_planned_ev_dis is not None and str(current_hour) in self.df_planned_ev_dis.columns:
                if i < len(self.df_planned_ev_dis):
                    ev_dis = self.df_planned_ev_dis.iloc[i][str(current_hour)] / 1000.0
                    
            evs_planned_log[ev_id] = ev_ch - ev_dis
            
            
            a_val = 0.0
            if self.alpha_data is not None and str(current_hour) in self.alpha_data.columns:
                if i < len(self.alpha_data):
                    a_val = self.alpha_data.iloc[i][str(current_hour)]
            
            alphas[ev_id] = int(float(a_val))
            
        return bess_planned, evs_planned_log, alphas

    def calculate_setpoints(self, current_hour, current_mode, pv_val, pl_val, bess_planned, evs_planned_log, alphas, preco_atual):
        """
        CENÁRIO B: DAY-AHEAD SCHEDULING (Blind Setpoints + Proteção de Rede).
        Tries to fulfill the optimizer's requested charge/discharge plan.
        If real-time grid conditions differ from the forecast (e.g., PV is lower), 
        it actively cuts the requested power to prevent the grid from exceeding P_GRID_MAX.
        """
        net_load = pl_val - pv_val
        margem_grid_disponivel = max(0.0, self.P_GRID_MAX - net_load)

        # ====================================================================
        # 2. EXECUÇÃO DO BESS 
        # ====================================================================
        bess_cmd = bess_planned
        max_ch_kwh = ((1.0 - self.current_soc) * self.BESS_CAPACITY) / self.bess_eff
        max_dis_kwh = (self.current_soc * self.BESS_CAPACITY) * self.bess_eff
        
        if bess_cmd > 0:
            # Tenta carregar o planeado, mas limitado pelo espaço físico e pela REDE
            bess_setpoint = min(bess_cmd, max_ch_kwh, self.BESS_MAX_CH, margem_grid_disponivel)
        elif bess_cmd < 0:
            # Descarga não consome margem de importação, logo não é limitada pelos 6.9 kW
            bess_setpoint = -min(abs(bess_cmd), max_dis_kwh, self.BESS_MAX_DIS)
        else:
            bess_setpoint = 0.0
            
        # Arredondamento
        if abs(bess_setpoint) < 0.1: bess_setpoint = 0.0
        elif bess_setpoint > 0: bess_setpoint = math.floor(bess_setpoint * 10) / 10.0
        else: bess_setpoint = math.ceil(bess_setpoint * 10) / 10.0
            
        net_load += bess_setpoint
        margem_grid_disponivel = max(0.0, self.P_GRID_MAX - net_load)

        # ====================================================================
        # 3. EXECUÇÃO DOS EVs (Tenta seguir o plano Day-Ahead)
        # ====================================================================
        ev_cmds = {ev_id: 0.0 for ev_id in self.ev_states.keys()}
        ev_acts = {ev_id: 0.0 for ev_id in self.ev_states.keys()}
        
        # Binários primeiro, porque se o corte da rede os afetar, caem logo para zero
        ev_items_ordenados = sorted(self.ev_states.items(), key=lambda item: item[1].get('is_binary', False), reverse=True)
        
        for ev_id, state in ev_items_ordenados:
            if alphas.get(ev_id, 0) == 0:
                continue
                
            ev_cmd_opt = evs_planned_log.get(ev_id, 0.0) # Usa a variável enviada pelo main!
            charger_max_ch = state['Pmax_ch']
            charger_max_dis = state['Pmax_dis']
            charger_min_ch = state.get('Pmin_ch', 0.0)
            is_binary = state.get('is_binary', False)
            
            ev_cmd = 0.0
            
            # ---> LÓGICA DO CENÁRIO B: Tentar fornecer exatamente a potência que o Otimizador pediu <---
            if ev_cmd_opt > 0:
                # O Otimizador quer carregar, mas a margem do quadro pode ser menor por falha de previsão
                ev_cmd = min(ev_cmd_opt, charger_max_ch, margem_grid_disponivel)
                
                # Regras rígidas dos carregadores
                if is_binary:
                    if ev_cmd >= charger_max_ch * 0.95:
                        ev_cmd = charger_max_ch
                    else:
                        ev_cmd = 0.0 # Cai a zero se a rede não aguentar!
                else:
                    if ev_cmd < charger_min_ch * 0.95:
                        ev_cmd = 0.0
                        
            elif ev_cmd_opt < 0:
                # O Otimizador quer descarregar (V2G/V2H)
                ev_cmd = -min(abs(ev_cmd_opt), charger_max_dis)
                if is_binary:
                    if abs(ev_cmd) >= charger_max_dis * 0.95:
                        ev_cmd = -charger_max_dis
                    else:
                        ev_cmd = 0.0

            # Arredondamentos
            if abs(ev_cmd) < 0.1: ev_cmd = 0.0
            if ev_cmd > 0 and not is_binary: ev_cmd = math.floor(ev_cmd * 10) / 10.0
            elif ev_cmd < 0 and not is_binary: ev_cmd = math.ceil(ev_cmd * 10) / 10.0
            
            ev_cmds[ev_id] = ev_cmd
            
            # Restrições Físicas da Bateria do Carro
            ev_max_ch_soc = ((1.0 - state['soc']) * state['Emax']) / state['eff']
            ev_max_dis_soc = (state['soc'] * state['Emax']) * state['dch_eff']
            
            ev_act = ev_cmd
            if ev_cmd > 0:
                ev_act = min(ev_cmd, ev_max_ch_soc)
                if is_binary and ev_act < charger_max_ch * 0.95:
                    ev_act = 0.0
            elif ev_cmd < 0:
                ev_act = -min(abs(ev_cmd), ev_max_dis_soc)
                if is_binary and abs(ev_act) < charger_max_dis * 0.95:
                    ev_act = 0.0
                    
            ev_acts[ev_id] = ev_act
            
            # Atualizar consumo da casa e reduzir a margem
            net_load += ev_act
            margem_grid_disponivel = max(0.0, self.P_GRID_MAX - net_load)
            
        return bess_setpoint, ev_cmds, ev_acts, net_load
        
    

    def publish_and_update_soc(self, bess_setpoint, ev_acts):
        """
        Calculates physical SoC transitions for the BESS and EVs using actual execution power 
        (accounting for hardware limits and efficiency loss).
        """
        # Update BESS
        transfer_bess = (bess_setpoint * 1.0) * self.bess_eff if bess_setpoint > 0 else (bess_setpoint * 1.0) / self.bess_eff
        self.current_soc = max(0.0, min(1.0, ((self.current_soc * self.BESS_CAPACITY) + transfer_bess) / self.BESS_CAPACITY))
        
        # Update EV Observer (Baseado na Ação Real, não no Comando)
        for ev_id, sp in ev_acts.items():
            state = self.ev_states[ev_id]
            if sp > 0:
                transfer = (sp * 1.0) * state['eff']
                logger.info(f"EV {ev_id}: ACTUAL CHARGE {sp} kW")
            elif sp < 0:
                transfer = (sp * 1.0) / state['dch_eff']
                logger.info(f"EV {ev_id}: ACTUAL DISCHARGE {abs(sp)} kW")
            else:
                transfer = 0.0
            
            nova_soc = max(0.0, min(1.0, ((state['soc'] * state['Emax']) + transfer) / state['Emax']))
            self.ev_states[ev_id]['soc'] = nova_soc

def main():
    """
    Extracts the user's selected scenario, calculates the real-world execution of the 
    pre-planned setpoints over a 24-hour cycle, aggregates financial costs, and visualizes 
    the system's physical constraints and behavior across multiple Matplotlib charts.
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
    
    try:
        df_css = pd.read_csv(os.path.join(pasta_medicoes, 'css_power.csv'), header=None)
        limite_global_kw = float(df_css.to_numpy().max()) / 1000.0
        if limite_global_kw < 1.0: limite_global_kw = 6.9 
        logger.info(f"Global Limit Loaded: {limite_global_kw} kW")
    except Exception as e:
        limite_global_kw = 6.9 
        logger.warning(f"Error reading css_power.csv: {e}. Using default limit of 6.9 kW.")
    
    controller = BessRealTimeController(
        modes_path=os.path.join(pasta_escolhida, 'Modos_Operacao_Analisados.csv'),
        pv_path=os.path.join(pasta_medicoes, 'pv.csv'),
        pl_path=os.path.join(pasta_medicoes, 'pl.csv'),
        cp_path=os.path.join(pasta_medicoes, 'cp_inputs.csv'),
        bess_path=os.path.join(pasta_medicoes, 'bess_inputs.csv'),
        prices_path=os.path.join(pasta_medicoes, 'energy_price.csv'), 
        pbess_ch_path=os.path.join(pasta_escolhida, 'PBess.csv'),
        pbess_dis_path=os.path.join(pasta_escolhida, 'PBessdc.csv'),
        evs_path=os.path.join(pasta_medicoes, 'evs_inputs.csv'),
        alpha_path=os.path.join(pasta_medicoes, 'alpha.csv'),
        pev_ch_path=os.path.join(pasta_escolhida, 'PEV.csv'),
        pev_dis_path=os.path.join(pasta_escolhida, 'PEVdc.csv'),
        p_grid_max=limite_global_kw
    )

    historico_resultados = []
    custo_total_diario = 0.0
    
    for hora_atual in range(1, 25):
        logger.info(f"--- [ HOUR {hora_atual:02d}:00 ] ---")
        # 1. Pede os dados e as previsões
        current_mode = controller.get_current_mode(hora_atual)
        pv_val, pl_val = controller.get_measurements_for_hour(hora_atual)
        bess_planned, evs_planned_log, alphas = controller.get_optimizer_setpoints(hora_atual)
        
        # 2. LER PREÇOS
        preco_atual_imp, preco_atual_exp = controller.get_price_for_hour(hora_atual)
        
        # 3. CHAMA O CONTROLADOR 
        bess_sp, ev_cmds, ev_acts, net_load_final = controller.calculate_setpoints(
            hora_atual, current_mode, pv_val, pl_val, bess_planned, evs_planned_log, alphas, preco_atual_imp
        )
        
        # 4. Atualiza os estados
        controller.publish_and_update_soc(bess_sp, ev_acts)
        
        # 5. Cálculo para o lado de fora do quadro (Rede)
        grid_import = max(0.0, net_load_final)
        grid_export = abs(min(0.0, net_load_final))
        
        # ---> CÁLCULO FINANCEIRO <---
        custo_hora = 0.0
        if preco_atual_imp is not None and preco_atual_exp is not None:
            # Como 1 passo = 1 hora, kW == kWh
            custo_hora = (grid_import * preco_atual_imp) - (grid_export * preco_atual_exp)
            custo_total_diario += custo_hora
        
        row_data = {
            'Hora': hora_atual,
            'Modo_Operacao': current_mode,
            'Preco_Imp': preco_atual_imp if preco_atual_imp is not None else 0.0, 
            'PV_kW': round(pv_val, 3), 'Load_kW': round(pl_val, 3),
            'Grid_Import_kW': round(grid_import, 3), 'Grid_Export_kW': round(grid_export, 3),
            'Custo_Hora_Euros': round(custo_hora, 4),
            'Opt_BESS_kW': round(bess_planned, 3),
            'Setpoint_BESS_kW': bess_sp, 'SOC_BESS_%': round(controller.current_soc * 100, 2)
        }
        
        # --- EV TRACKING ---
        for ev_id in controller.ev_states.keys():
            estado = controller.ev_states[ev_id]
            
            row_data[f'Alpha_EV{ev_id}'] = alphas.get(ev_id, 0) # Guarda se o EV está ligado (1) ou não (0)
            
            row_data[f'Opt_EV{ev_id}_kW'] = round(evs_planned_log.get(ev_id, 0), 3)
            row_data[f'Cmd_EV{ev_id}_kW'] = ev_cmds.get(ev_id, 0)
            row_data[f'Act_EV{ev_id}_kW'] = round(ev_acts.get(ev_id, 0), 3)
            row_data[f'SOC_EV{ev_id}_%'] = round(estado['soc'] * 100, 2)
            
            # Guardar a Energia e o Target em kWh para os Gráficos
            row_data[f'Energy_EV{ev_id}_kWh'] = round(estado['soc'] * estado['Emax'], 3)
            row_data[f'Target_Energy_EV{ev_id}_kWh'] = round(estado['target_soc'] * estado['Emax'], 3)
            
        historico_resultados.append(row_data)

    df_res = pd.DataFrame(historico_resultados)
    df_res.to_csv(os.path.join(pasta_escolhida, 'Resultados_RTO_Final.csv'), index=False)
    
    # ---> IMPRIMIR O CUSTO TOTAL DIÁRIO <---
    print("\n" + "="*50)
    logger.info(f"CUSTO TOTAL DO DIA (RTO): {custo_total_diario:.3f} €")
    print("="*50 + "\n")
    
    # =============================================================================
    # GERAÇÃO DE GRÁFICOS
    # =============================================================================

    time_steps = df_res['Hora'].values
    pl_w = df_res['Load_kW'].values
    pv_w = df_res['PV_kW'].values
    imp_w = df_res['Grid_Import_kW'].values
    exp_w = df_res['Grid_Export_kW'].values
    b_kw = df_res['Setpoint_BESS_kW'].values
    b_ch_w, b_dis_w = np.where(b_kw > 0, b_kw, 0), np.where(b_kw < 0, abs(b_kw), 0)
    
    ev_ch_w, ev_dis_w = np.zeros(24), np.zeros(24)
    for ev_id in controller.ev_states.keys():
        ev_kw = df_res[f'Act_EV{ev_id}_kW'].values
        ev_ch_w += np.where(ev_kw > 0, ev_kw, 0)
        ev_dis_w += np.where(ev_kw < 0, abs(ev_kw), 0)
        
    limite_w = limite_global_kw
    
    # =========================================================================
    # CONFIGURAÇÃO GERAL (Mantém o teu estilo LaTeX)
    # =========================================================================
    plt.rcParams['text.usetex'] = False  
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['mathtext.fontset'] = 'cm'  
    plt.rcParams.update({'font.size': 9})

    # -------------------------------------------------------------
    # 1. GRÁFICO DE BARRAS AGRUPADAS (Consumo vs Produção)
    # -------------------------------------------------------------
    fig1, ax1 = plt.subplots(figsize=(15, 7))
    
    # Eixo X: Definir o centro de cada hora (0.5, 1.5, ..., 23.5)
    x_centers = np.arange(0.5, 24.5, 1)
    width = 0.35  # Largura de cada barra
    
    # Deslocamento para colocar as barras lado a lado
    x_cons = x_centers - width/2  # Barras de Consumo à esquerda
    x_prod = x_centers + width/2  # Barras de Produção à direita
    
    # 1. DESENHAR AS BARRAS DE CONSUMO (Empilhadas)
    ax1.bar(x_cons, pl_w, width, label='Building Load', color='#74a0c2', edgecolor='black', linewidth=0.5, zorder=3)
    ax1.bar(x_cons, ev_ch_w, width, bottom=pl_w, label='EV Charge', color='#2e9bf5', edgecolor='black', linewidth=0.5, zorder=3)
    ax1.bar(x_cons, b_ch_w, width, bottom=pl_w + ev_ch_w, label='BESS Charge', color='#da70d6', edgecolor='black', linewidth=0.5, zorder=3)
    
    # 2. DESENHAR AS BARRAS DE PRODUÇÃO (Empilhadas)
    ax1.bar(x_prod, pv_w, width, label='PV Generation', color='#5fb060', edgecolor='black', linewidth=0.5, zorder=3)
    ax1.bar(x_prod, b_dis_w, width, bottom=pv_w, label='BESS Discharge', color='#8a2be2', edgecolor='black', linewidth=0.5, zorder=3)
    
    # 3. LINHAS DE REDE (Mantêm-se como Steps para cobrir a hora inteira)
    t_ext = np.insert(time_steps, 0, 0)
    imp_ext = np.insert(imp_w, 0, imp_w[0])
    exp_ext = np.insert(exp_w, 0, exp_w[0])
    
    s3 = pv_w + b_dis_w + ev_dis_w
    dynamic_ceiling_w = limite_w + s3
    dynamic_ceiling_ext = np.insert(dynamic_ceiling_w, 0, dynamic_ceiling_w[0])
    
    # Desenhar as linhas de importação/exportação
    ax1.plot(t_ext, imp_ext, color='orange', linewidth=2.5, drawstyle='steps-pre', zorder=4)
    ax1.plot(t_ext, exp_ext, color='purple', linewidth=2.5, drawstyle='steps-pre', zorder=4)
    
    # Marcadores centrados
    ax1.plot(x_centers, imp_w, color='orange', marker='o', markersize=5, linestyle='', zorder=5)
    ax1.plot(x_centers, exp_w, color='purple', marker='o', markersize=5, linestyle='', zorder=5)
    
    # Linhas vazias para a legenda (Rede)
    ax1.plot([], [], color='orange', linewidth=2.5, marker='o', markersize=6, label='Grid Import')
    ax1.plot([], [], color='purple', linewidth=2.5, marker='o', markersize=6, label='Grid Export')

    # 4. LIMITES DA REDE
    ax1.axhline(limite_w, color='red', linestyle='--', linewidth=1.5, label='Grid Limit', zorder=6)
    ax1.plot(t_ext, dynamic_ceiling_ext, color='darkred', linestyle='-.', linewidth=2.5, label='Total Available Power', drawstyle='steps-pre', zorder=6)
    
    # =========================================================================
    # 5. ESTILIZAÇÃO GERAL
    # =========================================================================
    ax1.grid(axis='y', linestyle='--', alpha=0.4, zorder=0)
    
    # Fixar a altura máxima do gráfico em 14 kW
    ax1.set_ylim(0, 14.0)
    
    ax1.set_xlim(0.0, 24)
    todas_as_horas = list(range(1, 25))
    etiquetas = [str(hora) if hora % 2 != 0 else '' for hora in todas_as_horas]
    
    ax1.set_xticks(x_centers)
    ax1.set_xticklabels(etiquetas)
    ax1.tick_params(axis='both', which='major', labelsize=14)
    
    ax1.set_ylabel('Power (kW)', fontsize=14, labelpad=10)
    ax1.set_xlabel('Time (Hour)', fontsize=14, labelpad=10)
    
    ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=5, framealpha=0.9, borderaxespad=0., fontsize=13)
    
    plt.tight_layout(pad=0.5)
    
    # Guardar a imagem
    fig1.savefig(os.path.join(pasta_escolhida, 'Grafico_Global_Barras_Agrupadas.png'), dpi=600, bbox_inches='tight', pad_inches=0.02)
    plt.close(fig1)

   # -------------------------------------------------------------
    # 2. GRÁFICOS ESPECÍFICOS PARA CADA EV (Lado a Lado + Legenda Única)
    # -------------------------------------------------------------
    ev_ids = list(controller.ev_states.keys())
    num_evs = len(ev_ids)

    if num_evs > 0:
        fig_ev, axes = plt.subplots(1, num_evs, figsize=(16, 6))
        
        if num_evs == 1:
            axes = [axes]

        axes_soc = []
        for i, (ax_pow, ev_id) in enumerate(zip(axes, ev_ids)):
            ax_soc = ax_pow.twinx()
            axes_soc.append(ax_soc)
            ax_price = ax_pow.twinx() 
            
            # Afastar o eixo do preço para a direita, para não sobrepor o da Energia
            ax_price.spines["right"].set_position(("axes", 1.15)) 
            
            # Adicionar Título para sabermos qual EV é qual
            ax_pow.set_title(f'EV {ev_id}', fontsize=19, fontweight='bold', pad=10)
            
            # 1. Extrair Dados de Potência
            opt_kw = df_res[f'Opt_EV{ev_id}_kW'].values
            act_kw = df_res[f'Act_EV{ev_id}_kW'].values
            alpha_ev = df_res[f'Alpha_EV{ev_id}'].values 
            
            # ---> ADICIONAR O SOMBREADO DA JANELA DE CONEXÃO <---
            for j in range(24):
                if alpha_ev[j] == 1:
                    ax_pow.axvspan(j + 0.5, j + 1.5, facecolor='lightgray', alpha=0.9, zorder=1, edgecolor='none')
            
            # Módulos para o eixo Y ficar sempre positivo
            opt_kw_abs = np.abs(opt_kw)
            act_kw_abs = np.abs(act_kw)
            
            # 2. Definir cores
            colors_opt = ['lightsteelblue' if val >= 0 else 'lightcoral' for val in opt_kw]
            colors_act = ['royalblue' if val >= 0 else 'crimson' for val in act_kw]
            
            # Posições das barras
            width = 0.4
            x = np.arange(1, 25)
            
            # 3. Gráfico de Barras Agrupadas
            ax_pow.bar(x - width/2, opt_kw_abs, width=width, color=colors_opt, edgecolor='black', linewidth=0.8, zorder=3)
            ax_pow.bar(x + width/2, act_kw_abs, width=width, color=colors_act, edgecolor='black', linewidth=0.8, zorder=3)
            
            # =========================================================
            # 4. LÓGICA DO SOC (Calcular Planeado vs Real)
            # =========================================================
            estado_ev = controller.ev_states[ev_id]
            soc_real_percent = df_res[f'SOC_EV{ev_id}_%'].values / 100.0
            
            # Reconstruir o SoC inicial (Hora 0)
            p_act_1 = act_kw[0]
            if p_act_1 > 0: transfer = p_act_1 * estado_ev['eff']
            elif p_act_1 < 0: transfer = p_act_1 / estado_ev['dch_eff']
            else: transfer = 0.0
            soc_0 = max(0.0, min(1.0, soc_real_percent[0] - (transfer / estado_ev['Emax'])))
            
            soc_real_full = np.insert(soc_real_percent, 0, soc_0)
            soc_opt_full = [soc_0]
            for j in range(24):
                p_opt = opt_kw[j]
                if p_opt > 0: t_opt = p_opt * estado_ev['eff']
                elif p_opt < 0: t_opt = p_opt / estado_ev['dch_eff']
                else: t_opt = 0.0
                next_soc = max(0.0, min(1.0, soc_opt_full[-1] + (t_opt / estado_ev['Emax'])))
                soc_opt_full.append(next_soc)
                
            x_soc = np.arange(0, 25)
            
            # ---> EXTRAIR E DESENHAR O PREÇO <---
            preco_array = df_res['Preco_Imp'].values
            preco_ext = np.insert(preco_array, 0, preco_array[0]) # Para casar com o bloco 0 da linha
            ax_price.plot(x_soc, preco_ext, color='darkorange', linestyle='--', marker='.', markersize=8, linewidth=2, zorder=5)
            
            # ---> CONVERSÃO PARA kWh <---
            energy_real_full = [val * estado_ev['Emax'] for val in soc_real_full]
            energy_opt_full = [val * estado_ev['Emax'] for val in soc_opt_full]
            
            # Desenhar as linhas de Energia (agora em kWh)
            ax_soc.plot(x_soc, energy_opt_full, color='gray', linestyle='--', marker='^', markersize=6, linewidth=2, zorder=4)
            ax_soc.plot(x_soc, energy_real_full, color='magenta', marker='o', markersize=6, linewidth=2, zorder=4)

            # =========================================================
            # 5. ESTILIZAÇÃO DO GRÁFICO (EIXOS)
            # =========================================================
            ax_pow.set_xlabel('Time (Hour)', fontsize=19)
            ax_pow.set_xlim(0.5, 24.5)
            todas_as_horas = list(range(1, 25))
            etiquetas = [str(hora) if hora % 2 != 0 else '' for hora in todas_as_horas]
            ax_pow.set_xticks(todas_as_horas)
            ax_pow.set_xticklabels(etiquetas)
            ax_pow.tick_params(axis='x', which='major', labelsize=17)
            
            # Limite da potência igual para todos
            ax_pow.set_ylim(0, 7.5) 
            
            # ---> ATUALIZAÇÃO DO EIXO DIREITO (ENERGIA) <---
            # O limite máximo de energia é a capacidade da bateria (Emax) + 5% de folga visual
            ax_soc.set_ylim(0.0, estado_ev['Emax'] * 1.05) 
            ax_soc.tick_params(axis='y', labelsize=17)
            
            # ---> ESTILIZAÇÃO DO 3º EIXO (PREÇO) <---
            max_preco = max(preco_array) if max(preco_array) > 0 else 0.20
            ax_price.set_ylim(0.0, max_preco * 1.1) # 10% de margem no topo
            
            if i == num_evs - 1: # Só mostra os números e o texto no ÚLTIMO gráfico (EV 2)
                ax_price.set_ylabel('Price (€/kWh)', fontsize=19, color='darkorange', labelpad=10)
                ax_price.tick_params(axis='y', labelsize=17, colors='darkorange')
            else:
                # Nos restantes gráficos (EV 1), oculta para não ficar no meio da imagem
                ax_price.tick_params(axis='y', right=False, labelright=False)
                ax_price.spines["right"].set_visible(False)
            
        # =========================================================
        # 6. LEGENDA ÚNICA PARTILHADA E CONFIGURAÇÃO DE EIXOS Y
        # =========================================================
        
        # 1. Configurar explicitamente o Gráfico da Esquerda (EV 1)
        axes[0].set_yticks([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        axes[0].set_yticklabels(['0', '1', '2', '3', '4', '5', '6', '7'])
        axes[0].set_ylabel('Power (kW)', fontsize=19, labelpad=15)
        axes[0].tick_params(axis='y', which='major', left=True, labelleft=True, labelsize=17)
        
        # 2. Configurar o Gráfico da Direita (EV 2), se existir
        if len(axes) > 1:
            for ax_extra in axes[1:]:
                ax_extra.set_ylabel('')
                ax_extra.set_yticks([0.0, 1.5, 3.0, 4.5, 6.0, 7.5])
                ax_extra.set_yticklabels([]) # Mata apenas o texto (Strings vazias)
                ax_extra.tick_params(axis='y', which='major', left=False, labelleft=False)

        # 3. Forçar o texto da Energia no Eixo Direito
        if len(axes_soc) > 1:
            axes_soc[0].set_ylabel('') # EV 1 fica limpinho sem texto
            axes_soc[-1].set_ylabel('Energy (kWh)', fontsize=19, labelpad=15) # Força o texto no EV 2
        else:
            axes_soc[0].set_ylabel('Energy (kWh)', fontsize=19, labelpad=15) # Se houver só 1 carro, escreve.

        legend_elements = [
            Patch(facecolor='lightsteelblue', edgecolor='black', label='Planned Charging Power'),
            #Patch(facecolor='lightcoral', edgecolor='black', label='Planned Discharging Power'),
            Patch(facecolor='royalblue', edgecolor='black', label='Executed Charging Power'),
            #Patch(facecolor='crimson', edgecolor='black', label='Executed Discharging Power'),
            Line2D([0], [0], color='gray', linestyle='--', marker='^', lw=2, label='Planned Stored Energy'),
            Line2D([0], [0], color='magenta', marker='o', lw=2, label='Executed Stored Energy'),
            Line2D([0], [0], color='darkorange', linestyle='--', marker='.', markersize=8, lw=2, label='Energy Price'),
            Patch(facecolor='lightgray', alpha=0.9, label='EV Connection Period')
        ]
        
        # 8 elementos ficam arrumados de forma perfeita em 4 colunas (2 linhas x 4 colunas)
        fig_ev.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, -0.02), ncol=3, framealpha=1, fontsize=17)
        
        # Usamos o w_pad para afastar um bocadinho o gráfico 1 do gráfico 2, para caber o 3º eixo
        plt.tight_layout(w_pad=3.0)
        
        # Guardar a imagem global conjunta (bbox_inches='tight' garante o corte perfeito das margens)
        nome_ficheiro = "Grafico_EVs_Comparacao_Global.png"
        caminho_final = os.path.join(pasta_escolhida, nome_ficheiro)
        fig_ev.savefig(caminho_final, dpi=600, bbox_inches='tight', pad_inches=0.02)
        plt.close(fig_ev)
             
             
    # -------------------------------------------------------------
    # 3. GRÁFICOS ESPECÍFICOS PARA CADA EV (Telemetria)
    # -------------------------------------------------------------
    for ev_id in controller.ev_states.keys():
        fig_ev, ax_pow = plt.subplots(figsize=(14, 6))
        ax_soc = ax_pow.twinx() # Eixo Duplo para o SoC
        
        # 1. Extrair Dados do Veículo
        alphas_ev = [df_res[f'Alpha_EV{ev_id}'].iloc[i] for i in range(24)]
        power_w = df_res[f'Act_EV{ev_id}_kW'].values * 1000
        
        # Criar uma versão puramente positiva (módulo) para a linha preta agregada
        power_w_abs = np.abs(power_w) 
        
        soc_percent = df_res[f'SOC_EV{ev_id}_%'].values / 100.0 # Converter para 0.0 a 1.0
        
        # 2. Separar as potências por Estado para pintar com cores diferentes
        pow_unplugged = np.full(24, np.nan)
        pow_idle = np.full(24, np.nan)
        pow_charging = np.full(24, np.nan)
        pow_discharging = np.full(24, np.nan)
        
        for i in range(24):
            if alphas_ev[i] == 0:
                pow_unplugged[i] = 0.0 # Força a zero para desenhar a linha vermelha no fundo
            elif power_w[i] == 0:
                pow_idle[i] = 0.0
            elif power_w[i] > 0:
                pow_charging[i] = power_w[i]
            elif power_w[i] < 0:
                # ---> MAGIA AQUI: Transforma a descarga (negativa) em positiva para o gráfico! <---
                pow_discharging[i] = abs(power_w[i]) 

      # --- TRUQUE: EXTENSÃO DOS ARRAYS PARA O BLOCO 0-1 ---
        t_ext = np.insert(time_steps, 0, 0)
        p_abs_ext = np.insert(power_w_abs, 0, power_w_abs[0])
        unp_ext = np.insert(pow_unplugged, 0, pow_unplugged[0])
        idle_ext = np.insert(pow_idle, 0, pow_idle[0])
        ch_ext = np.insert(pow_charging, 0, pow_charging[0])
        #dis_ext = np.insert(pow_discharging, 0, pow_discharging[0])

        # 3. Desenhar Eixo Esquerdo (Potência e Estados com steps-pre)
        ax_pow.plot(t_ext, p_abs_ext, color='black', linestyle='--', label='Aggregated (1 hour)', zorder=2, drawstyle='steps-pre')
        ax_pow.plot(t_ext, unp_ext, color='red', linewidth=3, label='UNPLUGGED', zorder=3, drawstyle='steps-pre')
        ax_pow.plot(t_ext, idle_ext, color='orange', linewidth=3, label='IDLE', zorder=3, drawstyle='steps-pre')
        ax_pow.plot(t_ext, ch_ext, color='green', linewidth=3, label='CHARGING', zorder=3, drawstyle='steps-pre')
        #ax_pow.plot(t_ext, dis_ext, color='blue', linewidth=3, label='DISCHARGING', zorder=3, drawstyle='steps-pre')
        
        # Os MARCADORES usam os arrays originais e recuam para o centro (- 0.5)
        ax_pow.plot(time_steps - 0.5, power_w_abs, color='black', marker='x', linestyle='', zorder=2)
       # ax_pow.plot(time_steps - 0.5, pow_unplugged, color='red', marker='o', markersize=6, linestyle='', zorder=3)
        ax_pow.plot(time_steps - 0.5, pow_idle, color='orange', marker='o', markersize=6, linestyle='', zorder=3)
        ax_pow.plot(time_steps - 0.5, pow_charging, color='green', marker='o', markersize=6, linestyle='', zorder=3)
        ax_pow.plot(time_steps - 0.5, pow_discharging, color='blue', marker='o', markersize=6, linestyle='', zorder=3)
        
        # 4. Desenhar Eixo Direito (SoC)
        # Calcular o SoC inicial (Hora 0) revertendo a operação da Hora 1
        p_act_1 = df_res[f'Act_EV{ev_id}_kW'].iloc[0]
        soc_1 = soc_percent[0]
        estado_ev = controller.ev_states[ev_id]
        
        if p_act_1 > 0:
            transfer = p_act_1 * estado_ev['eff']
        elif p_act_1 < 0:
            transfer = p_act_1 / estado_ev['dch_eff']
        else:
            transfer = 0.0
            
        soc_0 = soc_1 - (transfer / estado_ev['Emax'])
        soc_0 = max(0.0, min(1.0, soc_0)) # Garantir que fica entre 0 e 100%
        
        # Juntar o SoC da hora 0 ao início do array original
        soc_percent_ext = np.insert(soc_percent, 0, soc_0)
        
        # Desenhar com o t_ext (que agora vai do 0 ao 24 em conjunto com as potências)
        ax_soc.plot(t_ext, soc_percent_ext, color='magenta', marker='o', markersize=5, linewidth=2, label='SoC (%)', zorder=4)
        
        # 5. Estilização do Gráfico
        ax_pow.set_title(f'EV {ev_id} - Power & SoC Telemetry', fontsize=14, fontweight='bold', pad=15)
        ax_pow.set_xlabel('Time (Hour)', fontsize=11)
        ax_pow.set_ylabel('Power (W)', fontsize=11)
        max_power = max(np.max(power_w_abs), 7000) 
        ax_pow.set_ylim(-100, max_power * 1.2) 
        ax_pow.grid(True, linestyle='-', alpha=0.6)
        ax_soc.set_ylabel('SoC (%)', fontsize=11)
        ax_soc.set_ylim(0.0, 1.05) 
        
        # Eixo X encaixado no zero!
        ax_pow.set_xlim(0, 24)
        ax_pow.set_xticks(range(0, 25))
        
        # 6. Agrupar Legendas no Canto Superior Esquerdo
        handles_pow, labels_pow = ax_pow.get_legend_handles_labels()
        handles_soc, labels_soc = ax_soc.get_legend_handles_labels()
        
        # Remove eventuais linhas vazias da legenda
        valid_handles = []
        valid_labels = []
        for h, l in zip(handles_pow + handles_soc, labels_pow + labels_soc):
            valid_handles.append(h)
            valid_labels.append(l)

        ax_pow.legend(valid_handles, valid_labels, loc='upper left', framealpha=1, fontsize=9)
        
        plt.tight_layout()
        fig_ev.savefig(os.path.join(pasta_escolhida, f'Grafico_EV_{str(ev_id)}_Telemetria.png'), dpi=300)
        plt.close(fig_ev)
        
    logger.info("Simulação RTO concluída. Gráfico Global e Gráficos de EVs gerados com sucesso!")

if __name__ == '__main__':
    main()