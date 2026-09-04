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

class BessRealTimeController:
    """
    Case A Controller: Dumb Charging + BESS Self-Consumption.
    This simulates a baseline scenario where no optimization is used.
    EVs charge immediately at their maximum allowed rate as soon as they are plugged in, 
    limited only by the physical constraints of the charger and the main grid connection.
    """
    def __init__(self, pv_path, pl_path, bess_path, cp_path, evs_path, alpha_path, p_grid_max, prices_path):
        
        """
        Initializes the controller by loading grid limits, prices, environmental data, 
        and hardware constraints for the BESS and EVs.
        """
        
        self.P_GRID_MAX = p_grid_max 
        
        try:
            self.prices_data = pd.read_csv(prices_path) if prices_path and os.path.exists(prices_path) else None
            self.pv_data = pd.read_csv(pv_path, header=None)
            self.pl_data = pd.read_csv(pl_path, header=None)
            self.alpha_data = pd.read_csv(alpha_path, index_col=0 if 'Unnamed: 0' in pd.read_csv(alpha_path).columns else None)
            
            bess_df = pd.read_csv(bess_path)
            self.BESS_MAX_CH = bess_df['Pmax_charge_rate'].iloc[0] / 1000.0
            self.BESS_MAX_DIS = bess_df['Pmax_discharge_rate'].iloc[0] / 1000.0
            self.BESS_CAPACITY = bess_df['Emax'].iloc[0] / 1000.0
            self.current_soc = bess_df['initial_soc'].iloc[0]
            self.bess_eff = bess_df['eff'].iloc[0]
        except Exception as e:
            logger.error(f"Erro a ler ficheiros base: {e}")

        # Configurar limites e tipos dos pontos de carregamento
        cp_maxs = {}
        cp_types = {}
        if cp_path and os.path.exists(cp_path):
            cp_df = pd.read_csv(cp_path)
            cp_df.columns = cp_df.columns.str.strip()
            for _, row in cp_df.iterrows():
                cid = int(row.get('cp_id', 1))
                cp_maxs[cid] = float(row.get('Pcpmax', 7200)) / 1000.0
                cp_types[cid] = int(row.get('Type', 1))

        # Configurar EVs
        self.ev_states = {}
        evs_df = pd.read_csv(evs_path)
        evs_df.columns = evs_df.columns.str.strip()
        col_id = 'ev_id' if 'ev_id' in evs_df.columns else evs_df.columns[0]
        evs_df.set_index(col_id, inplace=True)
        
        for idx, row in evs_df.iterrows():
            ev_id = str(idx).strip()
            ficha = int(row.get('cpconnected', 1))
            ev_pmax = row['PchmaxEV'] / 1000.0
            
            # Identifica se é binário (Type 2 ou ID da ficha 2)
            is_bin = True if cp_types.get(ficha, 1) == 2 or ficha == 2 else False
            
            self.ev_states[ev_id] = {
                'Pmax_ch': min(ev_pmax, cp_maxs.get(ficha, ev_pmax)),  
                'Emax': row['EEVmax'] / 1000.0,
                'soc': (row['Esoc'] / 1000.0) / (row['EEVmax'] / 1000.0) if row['EEVmax'] > 0 else 0.0,
                'eff': row.get('evcheff', 0.95),
                'dch_eff': row.get('evdcheff', 0.95),
                'is_binary': is_bin,
                'idx_alpha': len(self.ev_states) # Guarda a linha original para cruzar com o alpha.csv
            }

    def get_measurements_for_hour(self, current_hour):
        """Fetches building load and solar generation in kW for the requested hour."""
        col_idx = current_hour - 1 
        pv_val = float(self.pv_data.iloc[-1 if self.pv_data.shape[0] < 24 else -1, col_idx]) / 1000.0 if self.pv_data is not None else 0.0
        pl_val = float(self.pl_data.iloc[-1 if self.pl_data.shape[0] < 24 else -1, col_idx]) / 1000.0 if self.pl_data is not None else 0.0
        return pv_val, pl_val
    
    def get_price_for_hour(self, current_hour):
        """Fetches dynamic grid import and export prices for the given hour."""
        if self.prices_data is None: return None, None
        col_idx = current_hour - 1
        p_imp = float(self.prices_data['import_price'].iloc[col_idx] if 'import_price' in self.prices_data.columns else self.prices_data.iloc[col_idx, 1])
        p_exp = float(self.prices_data['export_price'].iloc[col_idx] if 'export_price' in self.prices_data.columns else self.prices_data.iloc[col_idx, 2])
        return p_imp, p_exp

    def calculate_setpoints(self, current_hour, pv_val, pl_val):
        """
        Executes Dumb Charging logic. 
        EVs take priority and consume available grid capacity immediately. 
        After EVs are satisfied, the BESS covers any remaining home load or absorbs excess solar.
        """
        net_load = pl_val - pv_val
        margem_import = max(0.0, self.P_GRID_MAX - net_load)
        ev_acts = {ev_id: 0.0 for ev_id in self.ev_states.keys()}
        alphas = {}

        # 1. DUMB CHARGING (Binários têm prioridade na margem da rede)
        evs_ordenados = sorted(self.ev_states.items(), key=lambda item: item[1]['is_binary'], reverse=True)
        
        for ev_id, state in evs_ordenados:
            idx_alpha = state['idx_alpha']
            # Lê o alpha usando a linha original exata do carro
            alpha = float(self.alpha_data.iloc[idx_alpha][str(current_hour)]) if self.alpha_data is not None else 0
            alphas[ev_id] = alpha
            
            if alpha == 1 and state['soc'] < 1.0:
                # O que o EV precisa para chegar a 100%
                ev_max_ch_soc = ((1.0 - state['soc']) * state['Emax']) / state['eff']
                # O que o EV gostaria de puxar da rede
                ev_cmd = min(state['Pmax_ch'], margem_import)
                
                if state['is_binary']:

                    # Verifica se a rede dá o limite TODO (margem 5%) e se a bateria aguenta o bloco TODO
                    if ev_cmd >= state['Pmax_ch'] * 0.95 and ev_max_ch_soc >= state['Pmax_ch'] * 0.95:
                        ev_act = state['Pmax_ch']
                    else:
                        ev_act = 0.0
                else:
                    # REGRA CONTÍNUA (Puxa o que der)
                    ev_act = min(ev_cmd, ev_max_ch_soc)
                
                ev_acts[ev_id] = round(ev_act, 2)
                net_load += ev_act
                margem_import = max(0.0, self.P_GRID_MAX - net_load)

        # 2. BESS SELF-CONSUMPTION (Bateria compensa a net load restante)
        bess_setpoint = 0.0
        max_ch_soc = ((1.0 - self.current_soc) * self.BESS_CAPACITY) / self.bess_eff
        max_dis_soc = (self.current_soc * self.BESS_CAPACITY) * self.bess_eff

        if net_load < -0.001: # Excedente Solar
            bess_setpoint = min(abs(net_load), self.BESS_MAX_CH, max_ch_soc)
        elif net_load > 0.001: # Défice na Casa
            bess_setpoint = -min(net_load, self.BESS_MAX_DIS, max_dis_soc)

        net_load += bess_setpoint

        # 3. Atualizar SOCs físicos
        transfer_bess = (bess_setpoint) * self.bess_eff if bess_setpoint > 0 else (bess_setpoint) / self.bess_eff
        self.current_soc = max(0.0, min(1.0, ((self.current_soc * self.BESS_CAPACITY) + transfer_bess) / self.BESS_CAPACITY))
        
        for ev_id, act in ev_acts.items():
            state = self.ev_states[ev_id]
            transfer = act * state['eff'] if act > 0 else act / state['dch_eff']
            self.ev_states[ev_id]['soc'] = max(0.0, min(1.0, ((state['soc'] * state['Emax']) + transfer) / state['Emax']))

        return bess_setpoint, ev_acts, net_load, alphas

def main():
    """
    Reads inputs, runs the 24-hour dumb-charging loop, calculates total baseline costs, 
    exports a CSV, and generates visualizations for system performance.
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
    
    # Tentativa de ler o limite do quadro (Fallback para 6.9 kW)
    try:
        df_css = pd.read_csv(os.path.join(pasta_medicoes, 'css_power.csv'), header=None)
        limite_global_kw = float(df_css.to_numpy().max()) / 1000.0
        if limite_global_kw < 1.0: limite_global_kw = 6.9 
    except:
        limite_global_kw = 6.9 
    
    controller = BessRealTimeController(
        pv_path=os.path.join(pasta_medicoes, 'pv.csv'),
        pl_path=os.path.join(pasta_medicoes, 'pl.csv'),
        cp_path=os.path.join(pasta_medicoes, 'cp_inputs.csv'),
        bess_path=os.path.join(pasta_medicoes, 'bess_inputs.csv'),
        prices_path=os.path.join(pasta_medicoes, 'energy_price.csv'), 
        evs_path=os.path.join(pasta_medicoes, 'evs_inputs.csv'),
        alpha_path=os.path.join(pasta_medicoes, 'alpha.csv'),
        p_grid_max=limite_global_kw
    )

    historico = []
    custo_total = 0.0
    
    for hora in range(1, 25):
        pv_val, pl_val = controller.get_measurements_for_hour(hora)
        p_imp, p_exp = controller.get_price_for_hour(hora)
        
        bess_sp, ev_acts, net_load_final, alphas = controller.calculate_setpoints(hora, pv_val, pl_val)
        
        grid_import = max(0.0, net_load_final)
        grid_export = abs(min(0.0, net_load_final))
        custo_hora = (grid_import * (p_imp or 0)) - (grid_export * (p_exp or 0))
        custo_total += custo_hora
        
        row = {
            'Hora': hora, 'PV_kW': round(pv_val, 3), 'Load_kW': round(pl_val, 3),
            'Grid_Import_kW': round(grid_import, 3), 'Grid_Export_kW': round(grid_export, 3),
            'Custo_Hora': round(custo_hora, 4), 
            'Preco_Imp': p_imp or 0.0,
            'BESS_kW': round(bess_sp, 3), 'SOC_BESS_%': round(controller.current_soc * 100, 2)
        }
        for ev_id, act in ev_acts.items():
            row[f'EV{ev_id}_kW'] = round(act, 3)
            row[f'SOC_EV{ev_id}_%'] = round(controller.ev_states[ev_id]['soc'] * 100, 2)
            row[f'Alpha_EV{ev_id}'] = alphas[ev_id]
            
        historico.append(row)

    df_res = pd.DataFrame(historico)
    df_res.to_csv(os.path.join(pasta_escolhida, 'Resultados_Dumb_Charging.csv'), index=False)
    
    print("\n" + "="*50)
    logger.info(f"CUSTO TOTAL DO DIA (DUMB CHARGING): {custo_total:.3f} €")
    print("="*50 + "\n")
    
    # =============================================================================
    # GERAÇÃO DE GRÁFICOS
    # =============================================================================
    
    time_steps = df_res['Hora'].values
    pl_w = df_res['Load_kW'].values
    pv_w = df_res['PV_kW'].values
    imp_w = df_res['Grid_Import_kW'].values
    exp_w = df_res['Grid_Export_kW'].values
    b_kw = df_res['BESS_kW'].values
    b_ch_w, b_dis_w = np.where(b_kw > 0, b_kw, 0), np.where(b_kw < 0, abs(b_kw), 0)
    
    ev_ch_w, ev_dis_w = np.zeros(24), np.zeros(24)
    for ev_id in controller.ev_states.keys():
        ev_kw = df_res[f'EV{ev_id}_kW'].values
        ev_ch_w += np.where(ev_kw > 0, ev_kw, 0)
        ev_dis_w += np.where(ev_kw < 0, abs(ev_kw), 0)
        
    limite_w = limite_global_kw
    
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
    ax1.bar(x_cons, b_ch_w, width, bottom=pl_w + ev_ch_w, label='BESS Charge', color='#f5a9e1', edgecolor='black', linewidth=0.5, zorder=3)
    
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
    
    # Linhas vazias para a legenda
    ax1.plot([], [], color='orange', linewidth=2.5, marker='o', markersize=6, label='Grid Import')
    ax1.plot([], [], color='purple', linewidth=2.5, marker='o', markersize=6, label='Grid Export')

    # 4. LIMITES DA REDE
    ax1.axhline(limite_w, color='red', linestyle='--', linewidth=1.5, label='Grid Limit', zorder=6)
    ax1.plot(t_ext, dynamic_ceiling_ext, color='darkred', linestyle='-.', linewidth=2.5, label='Total Available Power', drawstyle='steps-pre', zorder=6)
    
    # 5. ESTILIZAÇÃO GERAL
    ax1.grid(axis='y', linestyle='--', alpha=0.4, zorder=0)
    
    # Fixar a altura máxima do gráfico em 14 kW
    ax1.set_ylim(0, 14.0)
    
    ax1.set_xlim(0.0, 24)
    todas_as_horas = list(range(1, 25))
    etiquetas = [str(hora) if hora % 2 != 0 else '' for hora in todas_as_horas]
    
    ax1.set_xticks(x_centers, labels=etiquetas)
    ax1.tick_params(axis='both', which='major', labelsize=14)
    
    ax1.set_ylabel('Power (kW)', fontsize=14, labelpad=10)
    ax1.set_xlabel('Time (Hour)', fontsize=14, labelpad=10)
    
    ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=5, framealpha=0.9, borderaxespad=0., fontsize=13)
    
    plt.tight_layout(pad=0.5)
    fig1.savefig(os.path.join(pasta_escolhida, 'Grafico_Barras_LadoALado_DC.png'), dpi=600, bbox_inches='tight', pad_inches=0.02)
    plt.close(fig1)

    # -------------------------------------------------------------
    # 2. GRÁFICOS ESPECÍFICOS PARA CADA EV (Dumb Charging - Caso Base)
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
            
            ax_pow.set_title(f'EV {ev_id}', fontsize=19, fontweight='bold', pad=10)
            
            # 1. Extrair Dados de Potência (Garante que apanha a coluna certa independentemente do nome)
            try:
                act_kw = df_res[f'EV{ev_id}_kW'].values
            except KeyError:
                act_kw = df_res[f'Act_EV{ev_id}_kW'].values 
                
            alpha_ev = df_res[f'Alpha_EV{ev_id}'].values 
            
            # Sombreado da janela de conexão
            for j in range(24):
                if alpha_ev[j] == 1:
                    ax_pow.axvspan(j + 0.5, j + 1.5, facecolor='lightgray', alpha=0.9, zorder=1, edgecolor='none')
            
            act_kw_abs = np.abs(act_kw)
            colors_act = ['royalblue' if val >= 0 else 'crimson' for val in act_kw]
            
            # Gráfico de Barras (Centrado no X, visto que não há barras 'Opt')
            width = 0.4
            x = np.arange(1, 25)
            ax_pow.bar(x, act_kw_abs, width=width, color=colors_act, edgecolor='black', linewidth=0.8, zorder=3)
            
            # =========================================================
            # 4. LÓGICA DA ENERGIA 
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
            x_soc = np.arange(0, 25)
            
            # ---> EXTRAIR E DESENHAR O PREÇO <---
            preco_array = df_res['Preco_Imp'].values
            preco_ext = np.insert(preco_array, 0, preco_array[0])
            ax_price.plot(x_soc, preco_ext, color='darkorange', linestyle='--', marker='.', markersize=8, linewidth=2, zorder=5)
            
            # Conversão para kWh e desenho
            energy_real_full = [val * estado_ev['Emax'] for val in soc_real_full]
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
            
            # Limite da potência
            ax_pow.set_ylim(0, 7.5) 
            
            # Limite máximo de energia (Capacidade Emax + 5%)
            ax_soc.set_ylim(0.0, estado_ev['Emax'] * 1.05) 
            ax_soc.tick_params(axis='y', labelsize=17)
            
            # Estilização do eixo do Preço
            max_preco = max(preco_array) if max(preco_array) > 0 else 0.20
            ax_price.set_ylim(0.0, max_preco * 1.1) 
            
            if i == num_evs - 1: # Só mostra os números no ÚLTIMO gráfico (EV 2)
                ax_price.set_ylabel('Price (€/kWh)', fontsize=19, color='darkorange', labelpad=10)
                ax_price.tick_params(axis='y', labelsize=17, colors='darkorange')
            else:
                ax_price.tick_params(axis='y', right=False, labelright=False)
                ax_price.spines["right"].set_visible(False)
            
        # =========================================================
        # 6. CONFIGURAÇÃO DOS EIXOS Y GLOBAIS E LEGENDA
        # =========================================================
        # Gráfico da Esquerda (EV 1)
        axes[0].set_yticks([0, 1, 2, 3, 4, 5, 6, 7])
        axes[0].set_yticklabels(['0', '1', '2', '3', '4', '5', '6', '7'])
        axes[0].set_ylabel('Power (kW)', fontsize=19, labelpad=15)
        axes[0].tick_params(axis='y', which='major', left=True, labelleft=True, labelsize=17)
        
        # Gráfico da Direita (EV 2), se existir
        if len(axes) > 1:
            for ax_extra in axes[1:]:
                ax_extra.set_ylabel('')
                ax_extra.set_yticks([0.0, 1.5, 3.0, 4.5, 6.0, 7.5])
                ax_extra.set_yticklabels([]) 
                ax_extra.tick_params(axis='y', which='major', left=False, labelleft=False)

        # Forçar o texto da Energia no Eixo Direito
        if len(axes_soc) > 1:
            axes_soc[0].set_ylabel('')
            axes_soc[-1].set_ylabel('Energy (kWh)', fontsize=19, labelpad=15) 
        else:
            axes_soc[0].set_ylabel('Energy (kWh)', fontsize=19, labelpad=15)

        legend_elements = [
            Patch(facecolor='royalblue', edgecolor='black', label='Executed EV Charging Power'),
            Line2D([0], [0], color='magenta', marker='o', lw=2, label='EV Stored Energy'),
            # Linha atualizada para corresponder a 100% com o que está no gráfico (tracejado e com ponto)
            Line2D([0], [0], color='darkorange', linestyle='--', marker='.', markersize=8, lw=2, label='Energy Price'),
            Patch(facecolor='lightgray', alpha=0.9, label='EV Connection Period')
        ]
        
        fig_ev.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, -0.02), ncol=3, framealpha=1, fontsize=17)
        
        # Espaçamento entre gráficos para caber o 3º eixo
        plt.tight_layout(w_pad=3.0)
        
        # Guardar (usar bbox_inches='tight' garante que o eixo do preço não é cortado da imagem)
        fig_ev.savefig(os.path.join(pasta_escolhida, "Grafico_EVs_Comparacao_Global_DC.png"), dpi=600, bbox_inches='tight', pad_inches=0.02)
        plt.close(fig_ev)
            
    # -------------------------------------------------------------
    # 3. GRÁFICOS ESPECÍFICOS PARA CADA EV (Telemetria)
    # -------------------------------------------------------------
    for ev_id in controller.ev_states.keys():
        fig_ev, ax_pow = plt.subplots(figsize=(14, 6))
        ax_soc = ax_pow.twinx()
        
        alphas_ev = df_res[f'Alpha_EV{ev_id}'].values
        power_w = df_res[f'EV{ev_id}_kW'].values * 1000
        power_w_abs = np.abs(power_w) 
        soc_percent = df_res[f'SOC_EV{ev_id}_%'].values / 100.0 
        
        pow_unplugged, pow_idle, pow_charging, pow_discharging = np.full(24, np.nan), np.full(24, np.nan), np.full(24, np.nan), np.full(24, np.nan)
        
        for i in range(24):
            if alphas_ev[i] == 0: pow_unplugged[i] = 0.0
            elif power_w[i] == 0: pow_idle[i] = 0.0
            elif power_w[i] > 0: pow_charging[i] = power_w[i]
            elif power_w[i] < 0: pow_discharging[i] = abs(power_w[i]) 

        t_ext = np.insert(time_steps, 0, 0)
        p_abs_ext = np.insert(power_w_abs, 0, power_w_abs[0])
        unp_ext = np.insert(pow_unplugged, 0, pow_unplugged[0])
        idle_ext = np.insert(pow_idle, 0, pow_idle[0])
        ch_ext = np.insert(pow_charging, 0, pow_charging[0])
        dis_ext = np.insert(pow_discharging, 0, pow_discharging[0])

        ax_pow.plot(t_ext, p_abs_ext, color='black', linestyle='--', label='Aggregated (1 hour)', zorder=2, drawstyle='steps-pre')
        ax_pow.plot(t_ext, unp_ext, color='red', linewidth=3, label='UNPLUGGED', zorder=3, drawstyle='steps-pre')
        ax_pow.plot(t_ext, idle_ext, color='orange', linewidth=3, label='IDLE', zorder=3, drawstyle='steps-pre')
        ax_pow.plot(t_ext, ch_ext, color='green', linewidth=3, label='CHARGING', zorder=3, drawstyle='steps-pre')
        ax_pow.plot(t_ext, dis_ext, color='blue', linewidth=3, label='DISCHARGING', zorder=3, drawstyle='steps-pre')
        
        ax_pow.plot(time_steps - 0.5, power_w_abs, color='black', marker='x', linestyle='', zorder=2)
        ax_pow.plot(time_steps - 0.5, pow_unplugged, color='red', marker='o', markersize=6, linestyle='', zorder=3)
        ax_pow.plot(time_steps - 0.5, pow_idle, color='orange', marker='o', markersize=6, linestyle='', zorder=3)
        ax_pow.plot(time_steps - 0.5, pow_charging, color='green', marker='o', markersize=6, linestyle='', zorder=3)
        ax_pow.plot(time_steps - 0.5, pow_discharging, color='blue', marker='o', markersize=6, linestyle='', zorder=3)
        
        p_act_1 = df_res[f'EV{ev_id}_kW'].iloc[0]
        soc_1 = soc_percent[0]
        estado_ev = controller.ev_states[ev_id]
        
        transfer = (p_act_1 * estado_ev['eff']) if p_act_1 > 0 else ((p_act_1 / estado_ev['dch_eff']) if p_act_1 < 0 else 0.0)
        soc_0 = max(0.0, min(1.0, soc_1 - (transfer / estado_ev['Emax'])))
        soc_percent_ext = np.insert(soc_percent, 0, soc_0)
        
        ax_soc.plot(t_ext, soc_percent_ext, color='magenta', marker='o', markersize=5, linewidth=2, label='SoC (%)', zorder=4)
        
        ax_pow.set_title(f'EV {ev_id} - Power & SoC Telemetry', fontsize=14, fontweight='bold', pad=15)
        ax_pow.set_xlabel('Time (Hour)', fontsize=11)
        ax_pow.set_ylabel('Power (W)', fontsize=11)
        max_power = max(np.max(power_w_abs), 7000) 
        ax_pow.set_ylim(-100, max_power * 1.2) 
        ax_pow.grid(True, linestyle='-', alpha=0.6)
        
        ax_soc.set_ylabel('SoC (%)', fontsize=11)
        ax_soc.set_ylim(0.0, 1.05) 
        
        ax_pow.set_xlim(0, 24)
        ax_pow.set_xticks(range(0, 25))
        
        handles_pow, labels_pow = ax_pow.get_legend_handles_labels()
        handles_soc, labels_soc = ax_soc.get_legend_handles_labels()
        
        valid_handles, valid_labels = [], []
        for h, l in zip(handles_pow + handles_soc, labels_pow + labels_soc):
            valid_handles.append(h)
            valid_labels.append(l)

        ax_pow.legend(valid_handles, valid_labels, loc='upper left', framealpha=1, fontsize=9)
        
        plt.tight_layout()
        fig_ev.savefig(os.path.join(pasta_escolhida, f'Grafico_EV_{str(ev_id).strip()}_Telemetria_DC.png'), dpi=300)
        plt.close(fig_ev)
        
    logger.info("Simulação Dumb Charging concluída. Gráficos gerados com sucesso!")

if __name__ == '__main__':
    main()