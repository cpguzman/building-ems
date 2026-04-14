import pandas as pd
import os
import glob

def definir_modo_operacao(row, p_grid_max):
    tol = 0.001
    
    # Extrair os valores da linha atual (com base nos nomes que vamos dar às colunas em baixo)
    p_grid_buy = row.get('Rede_Importacao_kW', 0)
    p_grid_sell = row.get('Rede_Exportacao_kW', 0)
    p_ch_ev = row.get('VE_Carga_kW', 0)
    p_dis_ev = row.get('VE_Descarga_kW', 0)
    p_ch_bat = row.get('BESS_Carga_kW', 0)
    p_dis_bat = row.get('BESS_Descarga_kW', 0)
    p_load = row.get('Instalacao_PL_kW', 0)
    
    p_ch_total = p_ch_bat + p_ch_ev
    p_dis_total = p_dis_bat + p_dis_ev
    
    if p_dis_total > tol and p_load > p_grid_max:
        return "PS"
    elif p_ch_total > tol and p_grid_buy > tol:
        return "ARB (Charge)"
    elif p_dis_total > tol and p_grid_sell > tol:
        return "ARB (Discharge)"
    elif p_ch_total > tol and p_grid_buy <= tol:
        return "SC (Charge)"
    elif p_dis_bat > tol and p_grid_sell <= tol:
        return "SC (Discharge)"
    else:
        return "IDLE"

def processar_modos_csv():
    # 1. Procurar todas as pastas de resultados
    pastas_resultados = glob.glob('RESULTS_*')
    
    if not pastas_resultados:
        print("Nenhuma pasta de resultados encontrada.")
        return

    # 2. Ler o consumo da instalação (PL).
  
    try:
        df_pl = pd.read_csv('pl.csv') 
        
        # Se o CSV tem as 3 fases nas linhas e o tempo nas colunas, somamos a coluna (axis=0)
        # Isto vai resultar numa série com 24 valores (um por hora)
        total_pl = df_pl.sum(axis=0) 
        
    except FileNotFoundError:
        print("Ficheiro 'pl.csv' não encontrado. O consumo PL será 0.")
        # Se falhar, criamos uma série de 24 zeros para não quebrar o código à frente
        total_pl = pd.Series([0]*24)

    P_GRID_MAX = 20000 

    for pasta in pastas_resultados:
        print(f"\nA processar os CSVs da pasta: {pasta}...")
        
        try:
            # 3. Ler os CSVs horários gerados pelo Pyomo (index_col=0 para usar o Instante como índice)
            grid_import = pd.read_csv(os.path.join(pasta, 'grid_import.csv'), index_col=0)
            grid_export = pd.read_csv(os.path.join(pasta, 'grid_export.csv'), index_col=0)
            pev_charge = pd.read_csv(os.path.join(pasta, 'PEV_h.csv'), index_col=0)
            pev_discharge = pd.read_csv(os.path.join(pasta, 'PEVdc_h.csv'), index_col=0)
            bess_charge = pd.read_csv(os.path.join(pasta, 'PBess_h.csv'), index_col=0)
            bess_discharge = pd.read_csv(os.path.join(pasta, 'PBessdc_h.csv'), index_col=0)
            
            # 4. Construir o DataFrame temporário (cruzar as colunas todas)
            # O .iloc[:, 0] garante que pegamos apenas na coluna de valores (ignorando o nome da coluna no CSV)
            df = pd.DataFrame({
                'Rede_Importacao_kW': grid_import.iloc[:, 0],
                'Rede_Exportacao_kW': grid_export.iloc[:, 0],
                'VE_Carga_kW': pev_charge.iloc[:, 0],
                'VE_Descarga_kW': pev_discharge.iloc[:, 0],
                'BESS_Carga_kW': bess_charge.iloc[:, 0],
                'BESS_Descarga_kW': bess_discharge.iloc[:, 0],
                'Instalacao_PL_kW': total_pl.values if isinstance(total_pl, pd.Series) else total_pl
            })
            
            # 5. Aplicar a tradução
            df['Modo_Operacao'] = df.apply(lambda row: definir_modo_operacao(row, P_GRID_MAX), axis=1)
           
            # Isolar apenas a coluna dos modos
            df_apenas_modos = df[['Modo_Operacao']]
               
            # 6. Guardar o resultado num novo CSV dentro da mesma pasta
            ficheiro_saida = os.path.join(pasta, 'Modos_Operacao_Analisados.csv')
               
            # Guardamos o novo DataFrame que só tem 1 coluna
            df_apenas_modos.to_csv(ficheiro_saida) 
               
            print(f"Tradução guardada em: {ficheiro_saida}")
            
        except FileNotFoundError as e:
            print(f"Erro ao processar a {pasta}: Faltam ficheiros CSV. ({e})")
            continue
        
        

# --- Execução ---
if __name__ == "__main__":
    processar_modos_csv()