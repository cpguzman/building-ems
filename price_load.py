import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import os
import matplotlib.patches as mpatches

# =========================================================================
# CONFIGURAÇÃO GERAL (Mantém o estilo LaTeX)
# =========================================================================
plt.rcParams['text.usetex'] = False  
plt.rcParams['font.family'] = 'serif'
plt.rcParams['mathtext.fontset'] = 'cm'  
plt.rcParams.update({'font.size': 9})

# Criar pasta para guardar as imagens geradas
folder = 'RESULTS_INPUTS'
if not os.path.exists(folder):
    os.makedirs(folder)

# =========================================================================
# LER OS DADOS DOS CSVs
# =========================================================================
try:
    df_pv = pd.read_csv('pv.csv', header=None)
    df_pl = pd.read_csv('pl.csv', header=None)
    df_price = pd.read_csv('energy_price.csv')
    
    # Lê a matriz alfa (1s e 0s)
    df_alpha = pd.read_csv('alpha.csv')
    if 'Unnamed: 0' in df_alpha.columns:
        df_alpha.set_index('Unnamed: 0', inplace=True)
        
except FileNotFoundError as e:
    print(f"Erro: Ficheiro CSV não encontrado. Certifica-te que os dados estão na mesma pasta. ({e})")
    exit()

# =========================================================================
# EXTRAIR OS ARRAYS DE VALORES 
# =========================================================================
# Se tiver 24 ou mais linhas, lê a última coluna. Se tiver menos (ex: 2), lê a última linha.
if df_pv.shape[0] >= 24:
    pv_vals = df_pv.iloc[:24, -1].values.astype(float) / 1000.0
else:
    pv_vals = df_pv.iloc[-1, :24].values.astype(float) / 1000.0

if df_pl.shape[0] >= 24:
    pl_vals = df_pl.iloc[:24, -1].values.astype(float) / 1000.0
else:
    pl_vals = df_pl.iloc[-1, :24].values.astype(float) / 1000.0

# Lê os preços (garantindo que pega apenas nas primeiras 24 horas)
import_price_vals = df_price.iloc[:24, 1].values.astype(float)
export_price_vals = df_price.iloc[:24, 2].values.astype(float)

time_steps = list(range(1, 25))
todas_as_horas = list(range(1, 25))
etiquetas = [str(hora) if hora % 2 != 0 else '' for hora in todas_as_horas]


# =========================================================================
# 1. GRÁFICO PV vs LOAD
# =========================================================================
print("A gerar Gráfico de PV vs Load...")
fig1, ax1 = plt.subplots(figsize=(8, 6))

ax1.plot(time_steps, pv_vals, label='PV Generation', color='green', marker='o', markersize=5, linestyle='-', linewidth=2)
ax1.fill_between(time_steps, 0, pv_vals, color='#5fb060', alpha=0.7)
ax1.plot(time_steps, pl_vals, label='Building Load', color='#1261A0', marker='o', markersize=5, linestyle='-', linewidth=2)
ax1.fill_between(time_steps, 0, pl_vals, color='#74a0c2', alpha=0.7)

# Linha dos 6.9 kW
ax1.axhline(6.9, color='red', linestyle='--', linewidth=1.5, label='Contracted Power', zorder=5)

ax1.set_ylabel('Power (kW)', fontsize=19)
ax1.set_xlabel('Time (Hour)', fontsize=19)
ax1.grid(True, linestyle=':', alpha=0.6)
ax1.axhline(0, color='black', linewidth=1)
ax1.set_xlim(1, 24)
ax1.set_xticks(todas_as_horas, labels=etiquetas)
ax1.tick_params(axis='both', which='major', labelsize=17)


# 1. Extrair os "handles" (linhas/cores) e os "labels" (textos) que foram desenhados
handles, labels = ax1.get_legend_handles_labels()

ordem = [0, 2, 1] 
handles_reordenados = [handles[i] for i in ordem]
labels_reordenados = [labels[i] for i in ordem]

# 2. Criar a legenda com as mesmas dimensões exatas da do Gráfico 2
ax1.legend(
    handles=handles_reordenados,
    labels=labels_reordenados,
    loc='upper center', 
    bbox_to_anchor=(0.5, -0.18), # Mesma posição do Gráfico 2
    ncol=2,                      # 2 colunas
    fontsize=16, 
    framealpha=0.9, 
    edgecolor='black'
)

plt.tight_layout()
plt.savefig(os.path.join(folder, 'Grafico_PV_Load.png'), dpi=300, bbox_inches='tight')
plt.close(fig1)


import matplotlib.patches as mpatches

# =========================================================================
# 2. GRÁFICO DOS PREÇOS DE ENERGIA (Barras Horizontais / ERSE Style)
# =========================================================================
print("A gerar Gráfico de Preços (Formato Barras)...")

# 1. Identificar os preços únicos e ordená-los
unique_import_prices = sorted(list(set(import_price_vals)))
unique_export_price = export_price_vals[0] 

# 2. Definir as Cores Oficiais
cores_import = ['#6aa84f', '#f1c232', '#cc0000'] 
cor_export = '#3d85c6' 

mapa_cores_import = {preco: cores_import[i % len(cores_import)] for i, preco in enumerate(unique_import_prices)}
nomes_tarifas = {0: 'Off-peak', 1: 'Peak', 2: 'Super Peak'}

# 3. Criar a figura
fig2, ax2 = plt.subplots(figsize=(8, 6))

legend_patches = []

# Criar a legenda da Importação
for i, preco in enumerate(unique_import_prices):
    nome = nomes_tarifas.get(i, f'Tarifa {i+1}')
    legend_patches.append(mpatches.Patch(color=mapa_cores_import[preco], label=f'{nome} ({preco:.3f} €)'))

# Criar a legenda da Exportação
legend_patches.append(mpatches.Patch(color=cor_export, label=f'Flat Rate ({unique_export_price:.3f} €)'))

# 4. Desenhar os blocos de hora a hora
for hour in range(1, 25):
    preco_imp = import_price_vals[hour-1]
    preco_exp = export_price_vals[hour-1]
    
    # Barra de Importação (Linha de Cima: y = 1)
    ax2.barh(1, width=1, left=hour-1, height=0.6, color=mapa_cores_import[preco_imp], edgecolor='white', linewidth=1)
    
    # Barra de Exportação (Linha de Baixo: y = 0)
    ax2.barh(0, width=1, left=hour-1, height=0.6, color=cor_export, edgecolor='white', linewidth=1)

# =========================================================================
# ESTILIZAÇÃO DO GRÁFICO
# =========================================================================
ax2.yaxis.tick_right()
ax2.set_yticks([0, 1])
ax2.set_yticklabels(['Export', 'Import'], fontsize=17)
ax2.tick_params(axis='y', length=0) # Remove os tracinhos do eixo Y, deixando só o texto

# Configuração do Eixo X 
ax2.set_xlim(0, 24)
ax2.set_xticks(todas_as_horas, labels=etiquetas)
ax2.set_xlabel('Time (Hour)', fontsize=19)
ax2.tick_params(axis='x', labelsize=17)

ax2.grid(axis='x', linestyle='--', alpha=0.7)
ax2.set_axisbelow(True)

# Remover as margens
ax2.spines['top'].set_visible(False)
ax2.spines['right'].set_visible(False)
ax2.spines['left'].set_visible(False)

# ---> ALTERAÇÃO AQUI: Legenda por baixo do gráfico
ax2.legend(
    handles=legend_patches, 
    loc='upper center',          # Âncora no topo da legenda
    bbox_to_anchor=(0.5, -0.18), # Posição abaixo do eixo X
    ncol=2,                      # 2 colunas para ficar simétrico (2x2)
    fontsize=16, 
    framealpha=0.9, 
    edgecolor='black'
)

plt.tight_layout()
plt.savefig(os.path.join(folder, 'Grafico_Price.png'), dpi=300, bbox_inches='tight')
plt.close(fig2)



# =========================================================================
# 3. GRÁFICO DE CONEXÕES DOS VEÍCULOS (Gantt Otimizado)
# =========================================================================
print("A gerar Gráfico de Conexão dos EVs...")
n_evs = len(df_alpha.index)

# Ajuste da altura da figura em função do número de EVs (mais elegante)
fig3, ax3 = plt.subplots(figsize=(9, max(3, 0.8 * n_evs)))


cores = ['#006400', '#90EE90']

y_ticks = []
y_labels = []

for i, ev_id in enumerate(df_alpha.index):
    # Posição simples no eixo Y (0, 1, 2...)
    y_pos = i  
    y_ticks.append(y_pos)
    
    cor_atual = cores[i % len(cores)]
    nome_carro = f'EV {str(ev_id+1).strip()}'
    y_labels.append(nome_carro)
    
    # Percorrer as 24 horas do ficheiro alpha.csv
    for hour in range(1, 25):
        try:
            is_connected = float(df_alpha.loc[ev_id, str(hour)])
        except KeyError:
            is_connected = float(df_alpha.loc[ev_id, hour])
            
        if is_connected == 1.0:
            # height=0.5 cria barras mais finas e elegantes
            # edgecolor='none' evita que as barras consecutivas fiquem fragmentadas
            ax3.barh(y_pos, width=1, left=hour-1, height=0.5, 
                     color=cor_atual, edgecolor='none', align='center')

# Configuração do Eixo Y (Colocar os nomes dos EVs diretamente no eixo)
ax3.set_yticks(y_ticks)
ax3.set_yticklabels(y_labels, fontsize=12)
ax3.invert_yaxis() # Inverte para que o EV 1 fique no topo (típico de Gantt)
ax3.tick_params(axis='y', length=0) # Remove os "risquinhos" do eixo Y 

# Configuração do Eixo X (Com números apenas nos ímpares)
ax3.set_xlim(0, 24)
todas_as_horas = list(range(1, 25))
etiquetas = [str(hora) if hora % 2 != 0 else '' for hora in todas_as_horas]
ax3.set_xticks(todas_as_horas, labels=etiquetas)
ax3.set_xlabel('Time (Hour)', fontsize=12)


# Grelha de fundo (apenas vertical, discreta)
ax3.grid(axis='x', linestyle=':', linewidth=1.2, alpha=0.6, color='#AAAAAA')
ax3.set_axisbelow(True) # Garante que a grelha fica atrás das barras

# Limpeza das margens (Spines)
ax3.spines['top'].set_visible(False)
ax3.spines['right'].set_visible(False)
ax3.spines['left'].set_visible(False)

plt.tight_layout()
plt.savefig(os.path.join(folder, 'Grafico_Conexao_EVs.png'), dpi=300, bbox_inches='tight')
plt.close(fig3)
