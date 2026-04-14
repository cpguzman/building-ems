import time
import os
import glob
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class BessRealTimeController:
    def __init__(self, modes_csv_path, pv_csv_path, pl_csv_path, bess_csv_path, p_grid_max):
        
        # 1. Carregar os Modos de Operação
        try:
            self.operation_modes = pd.read_csv(modes_csv_path, index_col=0)
            logger.info("Modos de operação carregados.")
        except FileNotFoundError:
            logger.error(f"Ficheiro de modos não encontrado: {modes_csv_path}")
            self.operation_modes = None
            
        # 2. Carregar Medições: PV  e PL (Consumo)
        try:
            # REMOVIDO o header=None. Vamos usar os cabeçalhos reais ("1", "2", "3"...)
            self.pv_data = pd.read_csv(pv_csv_path)
            self.pl_data = pd.read_csv(pl_csv_path)
            logger.info("Dados de PV e PL carregados (Formato Horizontal Detetado).")
        except FileNotFoundError:
            logger.error("Ficheiros pv.csv ou pl.csv não encontrados no diretório raiz.")
            self.pv_data = None
            self.pl_data = None

        # 3. Carregar Bateria Automaticamente
        try:
            bess_df = pd.read_csv(bess_csv_path)
            self.BESS_MAX_CH = bess_df['Pmax_charge_rate'].iloc[0] / 1000.0
            self.BESS_MAX_DIS = bess_df['Pmax_discharge_rate'].iloc[0] / 1000.0
            self.BESS_CAPACITY = bess_df['Emax'].iloc[0] / 1000.0
            self.current_soc = bess_df['initial_soc'].iloc[0]
            self.bess_eff = bess_df['eff'].iloc[0]
            logger.info(f"BESS Configurado: {self.BESS_CAPACITY} kWh | Carga/Descarga Max: {self.BESS_MAX_CH} kW | SOC Inicial: {self.current_soc*100}% | Eficiência: {self.bess_eff*100}%")
        except Exception as e:
            logger.error(f"Erro ao ler bess_inputs.csv: {e}")
            return

        self.P_GRID_MAX = p_grid_max              

    def get_current_mode(self, current_hour):
        if self.operation_modes is not None and current_hour in self.operation_modes.index:
            return self.operation_modes.loc[current_hour, 'Modo_Operacao']
        return "IDLE"

    def get_measurements_for_hour(self, current_hour):
        """ Vai buscar os dados horizontalmente pelas colunas ("1" a "24") e converte W para kW """
        pv_val, pl_val = 0.0, 0.0
        
        # O pandas lê os cabeçalhos "1", "2" como texto (strings).
        coluna = str(current_hour)
        
        try:
            if self.pv_data is not None: 
                # Pega no valor da coluna "1", "2", etc., na linha 0 e converte para kW
                pv_val = float(self.pv_data[coluna].iloc[0]) / 1000.0
                
            if self.pl_data is not None: 
                pl_val = float(self.pl_data[coluna].iloc[0]) / 1000.0
                
        except KeyError:
            logger.warning(f"Coluna '{coluna}' não encontrada nos ficheiros CSV.")
        except Exception as e:
            logger.warning(f"Erro ao ler dados da hora {current_hour}: {e}")
            
        return pv_val, pl_val
    
    def calculate_setpoint(self, current_mode, pv_power, load_power):
        setpoint_kw = 0.0
        
        # 1. LIMITES DINÂMICOS (Física da Bateria)
        # Assumindo passos de 1 hora: Potência (kW) = Energia (kWh) / 1h
        
        # Máxima potência que podemos pedir para CARREGAR antes de bater nos 100% de SOC
        max_kw_charge_soc = ((1.0 - self.current_soc) * self.BESS_CAPACITY) / self.bess_eff
        actual_max_charge = min(self.BESS_MAX_CH, max_kw_charge_soc)
        
        # Máxima potência que podemos pedir para DESCARREGAR antes de bater nos 5% de SOC
        max_kw_discharge_soc = ((self.current_soc - 0.05) * self.BESS_CAPACITY) * self.bess_eff
        actual_max_discharge = min(self.BESS_MAX_DIS, max_kw_discharge_soc)
        
        # Evitar valores negativos por erros de arredondamento
        actual_max_charge = max(0.0, actual_max_charge)
        actual_max_discharge = max(0.0, actual_max_discharge)

        # 2. LÓGICA DE MODOS DE OPERAÇÃO
        if current_mode == "PS":
            # A rede apenas vê o Net Load (Consumo - Produção Local)
            net_load = load_power - pv_power
            if net_load > self.P_GRID_MAX:
                excesso = net_load - self.P_GRID_MAX
                setpoint_kw = -min(excesso, actual_max_discharge)

        elif current_mode == "SC (Charge)":
            excedente_solar = pv_power - load_power
            if excedente_solar > 0:
                setpoint_kw = min(excedente_solar, actual_max_charge)

        elif current_mode == "SC (Discharge)":
            defice_solar = load_power - pv_power
            if defice_solar > 0:
                setpoint_kw = -min(defice_solar, actual_max_discharge)

        elif current_mode == "ARB (Charge)":
            setpoint_kw = actual_max_charge

        elif current_mode == "ARB (Discharge)":
            setpoint_kw = -actual_max_discharge

        return round(setpoint_kw, 3)

    def publish_and_update_soc(self, setpoint_kw):
        """ 
        Calcula a nova energia da bateria respeitando a eficiência (eff) no CSV.
        Assumi passos de 1 hora. Energia (kWh) = Potência (kW) * 1h.
        """
        if setpoint_kw > 0:
            # Durante o CARREGAMENTO, nem toda a energia da rede entra na bateria (perdas do inversor)
            energia_transferida = (setpoint_kw * 1.0) * self.bess_eff 
            logger.info(f"COMANDO: Bateria CARREGA a {setpoint_kw} kW")
            
        elif setpoint_kw < 0:
            # Durante o DESCARREGAMENTO, a bateria gasta mais internamente para entregar a potência pedida à rede
            energia_transferida = (setpoint_kw * 1.0) / self.bess_eff 
            logger.info(f"COMANDO: Bateria DESCARREGA a {abs(setpoint_kw)} kW")
            
        else:
            energia_transferida = 0.0
            logger.info(f"COMANDO: Bateria em STANDBY (0 kW)")

        # Atualizar a capacidade virtual
        nova_energia_kwh = (self.current_soc * self.BESS_CAPACITY) + energia_transferida
        self.current_soc = nova_energia_kwh / self.BESS_CAPACITY
        self.current_soc = max(0.0, min(1.0, self.current_soc)) # Travar entre 0% e 100%

        logger.info(f"Bateria Resultante: {self.current_soc*100:.1f} %")


# ==========================================
# SELEÇÃO DO MENU E ARRANQUE
# ==========================================

def main():
    pastas = glob.glob('RESULTS_*')
    if not pastas:
        print("Nenhuma pasta 'RESULTS_*' encontrada.")
        return
        
    print("\n" + "="*50 + "\n Pastas Disponíveis:\n" + "="*50)
    for i, pasta in enumerate(pastas): print(f"[{i}] - {pasta}")
    
    escolha = input("\nNumero da pasta para simular: ")
    pasta_escolhida = pastas[int(escolha)]
        
    
    caminho_modos = os.path.join(pasta_escolhida, 'Modos_Operacao_Analisados.csv')
    
    # --- Definir a pasta das medições e os caminhos corretos ---
    pasta_medicoes = 'measurements'
    caminho_pv = os.path.join(pasta_medicoes, 'pv.csv')
    caminho_pl = os.path.join(pasta_medicoes, 'pl.csv')
    caminho_bess = os.path.join(pasta_medicoes, 'bess_inputs.csv')
    
    # Criar Controlador e passar os caminhos
    controller = BessRealTimeController(
        modes_csv_path=caminho_modos,
        pv_csv_path=caminho_pv,
        pl_csv_path=caminho_pl,
        bess_csv_path=caminho_bess,
        p_grid_max=20.0  # Valor da potência contratada do quadro elétrico (em kW)
    )

    if controller.operation_modes is None or controller.pv_data is None: 
        return
        
    print("\n" + "="*60)
    logger.info("A INICIAR SIMULAÇÃO RTO - PV + PL + BATERIA")
    print("="*60 + "\n")
    
    # --- Lista para guardar o histórico da simulação ---
    historico_resultados = []
    
    for hora_atual in range(1, 25):
        logger.info(f"--- [ HORA {hora_atual:02d}:00 ] ---")
        
        current_mode = controller.get_current_mode(hora_atual)
        pv_real, load_real = controller.get_measurements_for_hour(hora_atual)
        
        logger.info(f"Sensores -> PV: {pv_real:.2f} kW | Load: {load_real:.2f} kW")
        logger.info(f"Plano Otimizador -> {current_mode}")
        
        setpoint = controller.calculate_setpoint(current_mode, pv_real, load_real)
        controller.publish_and_update_soc(setpoint)
        
        # --- Guardar os dados desta hora num dicionário ---
        historico_resultados.append({
            'Hora': hora_atual,
            'Modo_Operacao': current_mode,
            'PV_kW': round(pv_real, 3),
            'Load_kW': round(load_real, 3),
            'Setpoint_BESS_kW': setpoint,
            'SOC_Final_%': round(controller.current_soc * 100, 2)
        })
        
        print("\n")
        time.sleep(1.5) # Podes reduzir este tempo se quiseres que a simulação corra mais depressa

    # --- NOVO: Exportar os resultados para CSV no final da simulação ---
    df_resultados = pd.DataFrame(historico_resultados)
    
    # O ficheiro será guardado dentro da pasta 'RESULTS_X' que escolheste
    caminho_output = os.path.join(pasta_escolhida, 'Resultados_Simulacao_RTO.csv')
    df_resultados.to_csv(caminho_output, index=False)
    
    print("="*60)
    logger.info(f"Simulação concluída. Ficheiro guardado em: {caminho_output}")
    print("="*60 + "\n")

if __name__ == '__main__':
    main()
    
    
   
    def get_measurements_for_hour(self, current_hour):
        """ Vai buscar os dados horizontalmente pelas colunas ("1" a "24") e converte W para kW """
        pv_val, pl_val = 0.0, 0.0
        
        # O pandas lê os cabeçalhos "1", "2" como texto (strings).
        coluna = str(current_hour)
        
        try:
            if self.pv_data is not None: 
                # Pega no valor da coluna "1", "2", etc., na linha 0 e converte para kW
                pv_val = float(self.pv_data[coluna].iloc[0]) / 1000.0
                
            if self.pl_data is not None: 
                pl_val = float(self.pl_data[coluna].iloc[0]) / 1000.0
                
        except KeyError:
            logger.warning(f" Coluna '{coluna}' não encontrada nos ficheiros CSV.")
        except Exception as e:
            logger.warning(f" Erro ao ler dados da hora {current_hour}: {e}")
            
        return pv_val, pl_val
    
    

    