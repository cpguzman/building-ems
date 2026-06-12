import time
import os
import glob
import pandas as pd
import logging
import math
import matplotlib.pyplot as plt
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_value_from_df(df, row_id, col_val, default=0.0):
    if df is None: return default
    col_str = str(col_val)
    if col_str not in df.columns: return default
    for idx in df.index:
        if str(idx) == str(row_id):
            return float(df.loc[idx, col_str])
    return default

class BessRealTimeController:
    """
    Layer 3: Real-Time Controller (Monophasic + Blind EV SoC + Pure Mode-Driven).
    Executes Operational Modes based purely on power flows and grid limits.
    EVs ignore Optimizer setpoints and react purely to the semantic mode rules.
    """
    def __init__(self, modes_path, pv_path, pl_path, bess_path, pbess_ch_path, pbess_dis_path, cp_path,
                 evs_path, alpha_path, pev_ch_path, pev_dis_path, p_grid_max, prices_path):
        
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
                    cp_maxs[cp_id] = cp_max # <--- Guardar o máximo!
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
                
                # CRUZAMENTO DINÂMICO DOS DADOS
                ficha_ligada = int(row.get('cpconnected', 1))
                tipo_da_ficha = cp_types.get(ficha_ligada, 1) 
                is_bin = True if tipo_da_ficha == 2 else False
                
                # ---> LER E CRUZAR POTÊNCIAS MÁXIMAS <---
                ev_pmax_ch = row['PchmaxEV'] / 1000.0
                cp_pmax = cp_maxs.get(ficha_ligada, ev_pmax_ch)
                real_pmax_ch = min(ev_pmax_ch, cp_pmax) # Fica com o mais fraco
                
                ev_pmax_dis = row['PdchmaxEV'] / 1000.0
                real_pmax_dis = min(ev_pmax_dis, cp_pmax)
                
                # ---> LER E CRUZAR POTÊNCIAS MÍNIMAS <---
                ev_min_ch = 0.0
                if 'Min Charge (kW)' in row:
                    ev_min_ch = float(row['Min Charge (kW)'])
                elif 'Min Charge (W)' in row:
                    ev_min_ch = float(row['Min Charge (W)']) / 1000.0
                    
                cp_min_charge_kw = cp_mins.get(ficha_ligada, 0.0)
                real_pmin_ch = max(ev_min_ch, cp_min_charge_kw) # Fica com o mais exigente!
                
                self.ev_states[ev_id_limpo] = {
                    'Pmax_ch': real_pmax_ch,  
                    'Pmax_dis': real_pmax_dis,
                    'Pmin_ch': real_pmin_ch, # O Controlador agora sabe o mínimo exigido!
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
        if self.operation_modes is not None and current_hour in self.operation_modes.index:
            return self.operation_modes.loc[current_hour, 'Modo_Operacao']
        return "IDLE"

    def get_measurements_for_hour(self, current_hour):
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
        bess_ch = get_value_from_df(self.df_planned_bess_ch, self.df_planned_bess_ch.index[0] if self.df_planned_bess_ch is not None else 0, current_hour) / 1000.0
        bess_dis = get_value_from_df(self.df_planned_bess_dis, self.df_planned_bess_dis.index[0] if self.df_planned_bess_dis is not None else 0, current_hour) / 1000.0
        bess_planned = bess_ch - bess_dis

        evs_planned_log = {}
        alphas = {}
        for ev_id in self.ev_states.keys():
            ev_ch = get_value_from_df(self.df_planned_ev_ch, ev_id, current_hour) / 1000.0
            ev_dis = get_value_from_df(self.df_planned_ev_dis, ev_id, current_hour) / 1000.0
            evs_planned_log[ev_id] = ev_ch - ev_dis
            
            a_val = get_value_from_df(self.alpha_data, ev_id, current_hour, default=None)
            if a_val is None:
                a_val = get_value_from_df(self.alpha_data, self.alpha_data.index[0] if self.alpha_data is not None else 0, current_hour, default=0)
            alphas[ev_id] = int(a_val)
            
        return bess_planned, evs_planned_log, alphas

    def calculate_setpoints(self, current_hour, current_mode, pv_val, pl_val, bess_planned, alphas, preco_atual):
        # 1. Calcular a carga líquida da casa (Consumo - Produção Solar)
        net_load = pl_val - pv_val
        
        # ====================================================================
        # LIMITES FÍSICOS DA BATERIA (BESS)
        # ====================================================================
        # Calcula quanto a bateria consegue efetivamente carregar até chegar aos 100%
        max_ch_soc_bess = ((1.0 - self.current_soc) * self.BESS_CAPACITY) / self.bess_eff
        act_max_ch_bess = min(self.BESS_MAX_CH, max_ch_soc_bess) # Fica com o menor valor (Inversor vs Espaço Livre)
        
        # Calcula quanto a bateria consegue descarregar até chegar aos 5% de segurança
        max_dis_soc_bess = ((self.current_soc - 0.05) * self.BESS_CAPACITY) * self.bess_eff
        act_max_dis_bess = min(self.BESS_MAX_DIS, max_dis_soc_bess)

        # ====================================================================
        # MÓDULO 1: LÓGICA DE EXECUÇÃO DA BESS
        # ====================================================================
        # Esta função interna calcula a potência da BESS, respeitando as margens da rede
        def execute_bess(nl, m_imp, m_exp):
            b_sp = 0.0
            
            if current_mode == "PS": # Cortar Picos: descarrega apenas o excesso acima do limite
                if nl > self.P_GRID_MAX: 
                    b_sp = -min(nl - self.P_GRID_MAX, act_max_dis_bess)
                    
            elif current_mode == "SC (Charge)" and nl < 0: # Autoconsumo: absorve o sol que sobra
                b_sp = min(abs(nl), act_max_ch_bess, m_imp)
                
            elif current_mode in ["SC (Discharge)", "V2H"] and nl > 0: # Suprir a casa: descarrega para a carga
                b_sp = -min(nl, act_max_dis_bess)
                
            elif current_mode == "ARB (Pure Charge)": 
                b_sp = min(act_max_ch_bess, m_imp)
                
            elif current_mode == "ARB (Mixed Charge)": 
                b_sp = -min(act_max_dis_bess, m_exp)
                
            elif current_mode == "ARB (EV Charge Only)": 
                # O Otimizador quer a Bateria quieta para não gastar dinheiro desnecessário
                b_sp = 0.0
                    
            elif current_mode == "ARB (Discharge)": # Vender energia à rede
                b_sp = -min(act_max_dis_bess, m_exp)

            # Prevenção de micro-ciclos e arredondamento a 1 casa decimal
            if abs(b_sp) < 0.1: b_sp = 0.0
            if b_sp > 0: b_sp = math.floor(b_sp * 10) / 10.0
            elif b_sp < 0: b_sp = math.ceil(b_sp * 10) / 10.0
            return b_sp

        # ====================================================================
        # MÓDULO 2: LÓGICA DE EXECUÇÃO DOS EVs
        # ====================================================================
        # Esta função interna calcula a potência de todos os carros ligados
        def execute_evs(nl, m_imp, m_exp):
            cmds = {ev_id: 0.0 for ev_id in self.ev_states.keys()}
            acts = {ev_id: 0.0 for ev_id in self.ev_states.keys()}

            # 1. DLB (Dynamic Load Balancing) - Ordenação Estratégica
            # Carregadores Binários vão primeiro (precisam de blocos fixos de potência).
            ev_items_ordenados = sorted(self.ev_states.items(), key=lambda item: item[1].get('is_binary', False), reverse=True)

            # 2. Conta quantos carros contínuos estão ativos (para dividir a margem de forma justa)
            cont_ativos = sum(1 for ev, st in ev_items_ordenados if not st.get('is_binary', False) and alphas.get(ev, 0) == 1 and st['soc'] < 0.99)

            for ev_id, state in ev_items_ordenados:
                if alphas.get(ev_id, 0) == 0: continue

                charger_max_ch = state['Pmax_ch']
                charger_max_dis = state['Pmax_dis']
                is_binary = state.get('is_binary', False)

                # ---> Fair Share (Divisão Justa) para Carregadores Contínuos <---
                margem_alvo = m_imp
                if not is_binary and cont_ativos > 0 and current_mode not in ["PS", "ARB (Discharge)", "SC (Discharge)", "V2H"]:
                    margem_alvo = m_imp / cont_ativos

                ev_cmd = 0.0

                # Lógica baseada no Modo (usa a margem_alvo em vez da margem global)
                if current_mode == "PS":
                    if nl > self.P_GRID_MAX and charger_max_dis > 0: ev_cmd = -min(nl - self.P_GRID_MAX, charger_max_dis)
                elif current_mode == "SC (Charge)":
                    if nl < 0: ev_cmd = min(abs(nl), charger_max_ch, margem_alvo)
                elif current_mode in ["SC (Discharge)", "V2H"]:
                    if nl > 0 and charger_max_dis > 0: ev_cmd = -min(nl, charger_max_dis)
                elif current_mode in ["ARB (Pure Charge)", "ARB (Mixed Charge)", "ARB (EV Charge Only)"]:
                    ev_cmd = min(charger_max_ch, margem_alvo)
                elif current_mode == "ARB (Discharge)":
                    if charger_max_dis > 0: ev_cmd = -min(charger_max_dis, m_exp)

                # Regra Oportunista
                if ev_cmd == 0.0 and current_mode not in ["SC (Discharge)", "V2H", "PS", "ARB (Discharge)"]:
                    if nl < -0.1:
                        ev_cmd = min(abs(nl), charger_max_ch, margem_alvo)
                    elif preco_atual is not None and preco_atual <= 0.10:
                        ev_cmd = min(charger_max_ch, margem_alvo)
               
                # ---> APLICAÇÃO DA REGRA BINÁRIA E LIMITES MÍNIMOS <---
                charger_min_ch = state.get('Pmin_ch', 0.0) # Vai buscar o mínimo

                if is_binary:
                    if ev_cmd > 0: # REGRA BINÁRIA DE CARGA
                        if ev_cmd >= charger_max_ch * 0.95:  
                            ev_cmd = charger_max_ch
                        else:
                            ev_cmd = 0.0 
                    elif ev_cmd < 0: # REGRA BINÁRIA DE DESCARGA (Nova!)
                        if abs(ev_cmd) >= charger_max_dis * 0.95:
                            ev_cmd = -charger_max_dis
                        else:
                            ev_cmd = 0.0
                            
                # Regra para Contínuos: Se a potência é menor que o mínimo, desliga!
                elif not is_binary and ev_cmd > 0:
                    if ev_cmd < charger_min_ch * 0.95: 
                        ev_cmd = 0.0

                # Arredondamentos (Agora o arredondamento negativo também só se aplica a contínuos!)
                if abs(ev_cmd) < 0.1: ev_cmd = 0.0
                if ev_cmd > 0 and not is_binary: ev_cmd = math.floor(ev_cmd * 10) / 10.0
                elif ev_cmd < 0 and not is_binary: ev_cmd = math.ceil(ev_cmd * 10) / 10.0
                
                cmds[ev_id] = ev_cmd

                # ==============================================================
                # 2. REALIDADE FÍSICA (O SoC da Bateria do Carro aguenta?)
                # ==============================================================
                ev_max_ch_soc = ((1.0 - state['soc']) * state['Emax']) / state['eff']
                ev_max_dis_soc = (state['soc'] * state['Emax']) * state['dch_eff']

                ev_act = ev_cmd
                if ev_cmd > 0:
                    ev_act = min(ev_cmd, ev_max_ch_soc)
                    # Se for binário e a bateria já não conseguir engolir o bloco inteiro, aborta
                    if is_binary and ev_act < charger_max_ch * 0.95:
                        ev_act = 0.0
                        
                elif ev_cmd < 0:
                    ev_act = -min(abs(ev_cmd), ev_max_dis_soc)
                    # Se for binário e a bateria já não tiver energia para fornecer o bloco inteiro, aborta
                    if is_binary and abs(ev_act) < charger_max_dis * 0.95:
                        ev_act = 0.0

                acts[ev_id] = ev_act

                # A rede sente a alteração e atualiza a margem do quadro para o carro seguinte
                nl += ev_act
                m_imp = max(0.0, self.P_GRID_MAX - nl)
                m_exp = max(0.0, self.P_GRID_MAX + nl)

                # Se foi um contínuo a ser avaliado, reduzimos o contador para a próxima iteração
                if not is_binary and cont_ativos > 0:
                    cont_ativos -= 1

            return cmds, acts, nl, m_imp, m_exp

     # ====================================================================
        # ROTEAMENTO DINÂMICO DE PRIORIDADE (Cérebro Principal)
        # ====================================================================
        # 1. Calcula as margens iniciais de energia que o Quadro Elétrico ainda suporta
        margem_import = max(0.0, self.P_GRID_MAX - net_load)
        margem_export = max(0.0, self.P_GRID_MAX + net_load)
        
        bess_setpoint = 0.0
        ev_cmds = {ev_id: 0.0 for ev_id in self.ev_states.keys()}
        ev_acts = {ev_id: 0.0 for ev_id in self.ev_states.keys()}
        
        modos_ev_primeiro = ["ARB (Pure Charge)", "ARB (EV Charge Only)", "SC (Charge)"]
        
        if current_mode in modos_ev_primeiro or (current_mode == "IDLE" and net_load < 0):

            # Primeiro, o EV usa a margem da rede
            ev_cmds, ev_acts, net_load, margem_import, margem_export = execute_evs(net_load, margem_import, margem_export)
            # Segundo, a BESS só carrega/descarrega usando a margem que o EV deixou sobrar
            bess_setpoint = execute_bess(net_load, margem_import, margem_export)
            net_load += bess_setpoint
            
        # MODO DE DESCARGA E MIXED: Bateria (BESS) atua primeiro (quer para vender, quer para dar "ajuda" aos EVs)
        else:
            # Primeiro, a BESS descarrega
            bess_setpoint = execute_bess(net_load, margem_import, margem_export)
            net_load += bess_setpoint
            
            # Recalcula as margens da rede com a injeção da BESS (Aumenta o espaço para os EVs!)
            margem_import = max(0.0, self.P_GRID_MAX - net_load)
            margem_export = max(0.0, self.P_GRID_MAX + net_load)
            
            # Segundo, os EVs entram e veem a margem expandida (Rede + BESS)
            ev_cmds, ev_acts, net_load, margem_import, margem_export = execute_evs(net_load, margem_import, margem_export)

        return bess_setpoint, ev_cmds, ev_acts, net_load

    def publish_and_update_soc(self, bess_setpoint, ev_acts):
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
            hora_atual, current_mode, pv_val, pl_val, bess_planned, alphas, preco_atual_imp
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
            'PV_kW': round(pv_val, 3), 'Load_kW': round(pl_val, 3),
            'Grid_Import_kW': round(grid_import, 3), 'Grid_Export_kW': round(grid_export, 3),
            'Custo_Hora_Euros': round(custo_hora, 4),
            'Opt_BESS_kW': round(bess_planned, 3),
            'Setpoint_BESS_kW': bess_sp, 'SOC_BESS_%': round(controller.current_soc * 100, 2)
        }
        
        # --- EV TRACKING ---
        for ev_id in controller.ev_states.keys():
            estado = controller.ev_states[ev_id]
            
            # ADICIONA ESTA LINHA ABAIXO:
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
    pl_w = df_res['Load_kW'].values * 1000
    pv_w = df_res['PV_kW'].values * 1000
    imp_w = df_res['Grid_Import_kW'].values * 1000
    exp_w = df_res['Grid_Export_kW'].values * 1000
    b_kw = df_res['Setpoint_BESS_kW'].values
    b_ch_w, b_dis_w = np.where(b_kw > 0, b_kw*1000, 0), np.where(b_kw < 0, abs(b_kw)*1000, 0)
    
    ev_ch_w, ev_dis_w = np.zeros(24), np.zeros(24)
    for ev_id in controller.ev_states.keys():
        ev_kw = df_res[f'Act_EV{ev_id}_kW'].values
        ev_ch_w += np.where(ev_kw > 0, ev_kw*1000, 0)
        ev_dis_w += np.where(ev_kw < 0, abs(ev_kw)*1000, 0)
        
    modos = df_res['Modo_Operacao'].tolist()
    limite_w = limite_global_kw * 1000

   # -------------------------------------------------------------
    # 1. GRÁFICO ÁREA EMPILHADA 
    # -------------------------------------------------------------
    fig1, ax1 = plt.subplots(figsize=(15, 7))
    ax1.set_title('Power Balance', fontsize=14, fontweight='bold', pad=30)
    
    d1, d2, d3 = pl_w, pl_w + ev_ch_w, pl_w + ev_ch_w + b_ch_w
    s1, s2, s3 = pv_w, pv_w + b_dis_w, pv_w + b_dis_w + ev_dis_w

    # --- TRUQUE: EXTENSÃO DOS ARRAYS PARA O BLOCO 0-1 (step='pre') ---
    # Inserimos um zero no início do eixo X
    t_ext = np.insert(time_steps, 0, 0) 
    
    # Repetimos o primeiro valor no início de todos os arrays
    d1_ext, d2_ext, d3_ext = np.insert(d1, 0, d1[0]), np.insert(d2, 0, d2[0]), np.insert(d3, 0, d3[0])
    s1_ext, s2_ext, s3_ext = np.insert(s1, 0, s1[0]), np.insert(s2, 0, s2[0]), np.insert(s3, 0, s3[0])
    imp_ext, exp_ext = np.insert(imp_w, 0, imp_w[0]), np.insert(exp_w, 0, exp_w[0])

    # Usamos step='pre' (o degrau forma-se ANTES da hora)
    ax1.fill_between(t_ext, 0, d1_ext, label='House Load', color='#74a0c2', alpha=0.8, step='pre')
    ax1.fill_between(t_ext, d1_ext, d2_ext, label='EV Charge', color='#2e9bf5', alpha=0.8, step='pre')
    ax1.fill_between(t_ext, d2_ext, d3_ext, label='BESS Charge', color='#da70d6', alpha=0.6, step='pre')
    
    ax1.fill_between(t_ext, 0, s1_ext, label='PV Generation', color='#5fb060', alpha=0.7, step='pre')
    ax1.fill_between(t_ext, s1_ext, s2_ext, label='BESS Discharge', color='#8a2be2', alpha=0.5, step='pre')
    ax1.fill_between(t_ext, s2_ext, s3_ext, label='EV Discharge', color='#dc143c', alpha=0.5, step='pre')
    
    # Linhas da Rede usam drawstyle='steps-pre'
    ax1.plot(t_ext, imp_ext, label='Grid Import', color='orange', linewidth=2, drawstyle='steps-pre')
    ax1.plot(t_ext, exp_ext, label='Grid Export', color='grey', linewidth=2, drawstyle='steps-pre')
    
    # As Bolinhas voltam para TRÁS meia hora (- 0.5) para ficar no centro do bloco!
    ax1.plot(time_steps - 0.5, imp_w, color='orange', marker='o', linestyle='')
    ax1.plot(time_steps - 0.5, exp_w, color='grey', marker='o', linestyle='')
    
    ax1.axhline(limite_w, color='red', linestyle='--', linewidth=1.5, label='Contracted Power', zorder=5)
    
    global_max_1 = max(np.max(d3), np.max(s3), np.max(imp_w), limite_w)
    if global_max_1 == 0: global_max_1 = 1000 
    ax1.set_ylim(0, global_max_1 * 1.35)
    
    # O limite do X agora fica perfeitamente encaixado entre a hora 0 e a hora 24!
    ax1.set_xlim(0, 24) 
    ax1.set_xticks(range(0, 25))
    
    for i, m in enumerate(modos): 
        y_max_local = max(d3[i], s3[i], imp_w[i])
        y_offset = y_max_local + (global_max_1 * 0.03)
        # O texto ganha - 0.5 no X para alinhar com as bolinhas
        ax1.text(time_steps[i] - 0.5, y_offset, m, rotation=90, ha='center', va='bottom', fontsize=8, fontweight='bold')
    
    ax1.set_ylabel('Power (W)')
    ax1.set_xlabel('Time (Hour)')
    ax1.legend(loc='upper left', bbox_to_anchor=(1,1))

    ax1.grid(True, alpha=0.3)

    
    plt.tight_layout()
    fig1.savefig(os.path.join(pasta_escolhida, 'Grafico_Area_Empilhada.png'), dpi=300)
    plt.close(fig1)

   # -------------------------------------------------------------
    # 2. GRÁFICOS ESPECÍFICOS PARA CADA EV (Planeado vs Real)
    # -------------------------------------------------------------

    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D

    for ev_id in controller.ev_states.keys():
        fig_ev, ax_pow = plt.subplots(figsize=(14, 6))
        ax_soc = ax_pow.twinx() # Criar eixo secundário para o SoC
        
        # 1. Extrair Dados de Potência
        opt_kw = df_res[f'Opt_EV{ev_id}_kW'].values
        act_kw = df_res[f'Act_EV{ev_id}_kW'].values
        
        # Módulos para o eixo Y ficar sempre positivo
        opt_kw_abs = np.abs(opt_kw)
        act_kw_abs = np.abs(act_kw)
        
        # 2. Definir cores (Azuis para Carga, Vermelhos para Descarga)
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
        
        # Array Real de SoC (Horas 0 a 24)
        soc_real_full = np.insert(soc_real_percent, 0, soc_0)
        
        # Recalcular o Array Planeado de SoC pelo Pyomo (Horas 0 a 24)
        soc_opt_full = [soc_0]
        for i in range(24):
            p_opt = opt_kw[i]
            if p_opt > 0: t_opt = p_opt * estado_ev['eff']
            elif p_opt < 0: t_opt = p_opt / estado_ev['dch_eff']
            else: t_opt = 0.0
            next_soc = max(0.0, min(1.0, soc_opt_full[-1] + (t_opt / estado_ev['Emax'])))
            soc_opt_full.append(next_soc)
            
        x_soc = np.arange(0, 25)
        
        # Desenhar as linhas de SoC no eixo secundário
        ax_soc.plot(x_soc, soc_opt_full, color='gray', linestyle='--', marker='^', markersize=6, linewidth=2, zorder=4)
        ax_soc.plot(x_soc, soc_real_full, color='magenta', marker='o', markersize=6, linewidth=2, zorder=4)

        # =========================================================
        # 5. ESTILIZAÇÃO DO GRÁFICO
        # =========================================================
        ax_pow.set_title(f'EV {ev_id} - Planned vs Executed Charge', fontsize=15, fontweight='bold', pad=15)
        ax_pow.set_xlabel('Time (Hour)', fontsize=12)
        ax_pow.set_ylabel('Power (kW)', fontsize=12)
        
        max_val = max(np.max(opt_kw_abs), np.max(act_kw_abs), 7.0)
        ax_pow.set_ylim(0, max_val * 1.3) # Folga no topo para a legenda
        ax_pow.set_xlim(0, 24.5)
        ax_pow.set_xticks(range(1, 25))
        ax_pow.grid(True, axis='y', linestyle='--', alpha=0.6, zorder=0)
        
        # Eixo SOC
        ax_soc.set_ylabel('SoC (%)', fontsize=12)
        ax_soc.set_ylim(0.0, 1.05)
        
        # Legendas Customizadas (Obrigatório porque as barras têm várias cores)
        legend_elements = [
            Patch(facecolor='lightsteelblue', edgecolor='black', label='Planned: Charge'),
            Patch(facecolor='lightcoral', edgecolor='black', label='Planned: Discharge'),
            Patch(facecolor='royalblue', edgecolor='black', label='Executed: Charge'),
            Patch(facecolor='crimson', edgecolor='black', label='Executed: Discharge'),
            Line2D([0], [0], color='gray', linestyle='--', marker='^', lw=2, label='Planned SoC (%)'),
            Line2D([0], [0], color='magenta', marker='o', lw=2, label='Executed SoC (%)')
        ]
        
        ax_pow.legend(handles=legend_elements, loc='upper left', framealpha=1, fontsize=10)
        
        plt.tight_layout()
        
        # Guardar o ficheiro
        nome_ficheiro = f"Grafico_EV_{str(ev_id).strip()}_Comparacao.png"
        caminho_final = os.path.join(pasta_escolhida, nome_ficheiro)
        fig_ev.savefig(caminho_final, dpi=300)
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
        dis_ext = np.insert(pow_discharging, 0, pow_discharging[0])

        # 3. Desenhar Eixo Esquerdo (Potência e Estados com steps-pre)
        ax_pow.plot(t_ext, p_abs_ext, color='black', linestyle='--', label='Aggregated (1 hour)', zorder=2, drawstyle='steps-pre')
        ax_pow.plot(t_ext, unp_ext, color='red', linewidth=3, label='UNPLUGGED', zorder=3, drawstyle='steps-pre')
        ax_pow.plot(t_ext, idle_ext, color='orange', linewidth=3, label='IDLE', zorder=3, drawstyle='steps-pre')
        ax_pow.plot(t_ext, ch_ext, color='green', linewidth=3, label='CHARGING', zorder=3, drawstyle='steps-pre')
        ax_pow.plot(t_ext, dis_ext, color='blue', linewidth=3, label='DISCHARGING', zorder=3, drawstyle='steps-pre')
        
        # Os MARCADORES usam os arrays originais e recuam para o centro (- 0.5)
        ax_pow.plot(time_steps - 0.5, power_w_abs, color='black', marker='x', linestyle='', zorder=2)
        ax_pow.plot(time_steps - 0.5, pow_unplugged, color='red', marker='o', markersize=6, linestyle='', zorder=3)
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
        fig_ev.savefig(os.path.join(pasta_escolhida, f'Grafico_EV_{str(ev_id).strip()}_Telemetria.png'), dpi=300)
        plt.close(fig_ev)
        
    logger.info("Simulação RTO concluída. Gráfico Global e Gráficos de EVs gerados com sucesso!")

if __name__ == '__main__':
    main()