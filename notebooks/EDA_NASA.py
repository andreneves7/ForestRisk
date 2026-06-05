"""
ForestRisk — EDA Automático com Relatório HTML
===============================================
Uso:
    python eda_relatorio.py                          # detecta CSVs automaticamente
    python eda_relatorio.py ficheiro_snpp.csv        # ficheiro único
    python eda_relatorio.py snpp.csv noaa20.csv      # dois satélites

Gera: eda_relatorio.html  (abre no browser)
"""

import sys
import os
import base64
import io
import warnings
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # sem janelas gráficas — guarda directo para memória
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

warnings.filterwarnings('ignore')
sns.set_theme(style='whitegrid', palette='husl')
plt.rcParams['figure.figsize'] = (12, 5)
plt.rcParams['figure.dpi'] = 120

# ─────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────
LAT_MIN, LAT_MAX = 36.9, 42.2
LON_MIN, LON_MAX = -9.5, -6.2
MESES_PT = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']
OUTPUT_HTML    = 'eda_relatorio.html'
PASTA_INPUT    = Path('NASACSV')           # CSVs originais da NASA
PASTA_CSV      = Path('Filtragem_CSV')     # CSVs limpos por ano
PASTA_PARQUET  = Path('Filtragem_Parquet') # Parquet limpos por ano

# ─────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────
def fig_para_base64():
    """Converte o gráfico activo para string base64 embutível em HTML."""
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=120, bbox_inches='tight')
    plt.close()
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')

def img_html(b64, titulo=""):
    return f'''
    <div class="grafico">
        <p class="grafico-titulo">{titulo}</p>
        <img src="data:image/png;base64,{b64}" alt="{titulo}">
    </div>'''

def tabela_html(df, id_css=""):
    return df.to_html(classes=f'tabela {id_css}', border=0, index=True)

def secao(titulo, nivel=2):
    tag = f'h{nivel}'
    return f'<{tag}>{titulo}</{tag}>'

# ─────────────────────────────────────────────
# CARREGAR DADOS
# ─────────────────────────────────────────────
def detectar_csvs():
    """Detecta CSVs FIRMS na pasta NASACSV."""
    if not PASTA_INPUT.exists():
        print(f"⚠️  Pasta '{PASTA_INPUT}' não encontrada.")
        print(f"   Cria a pasta NASACSV e coloca lá os ficheiros NASA FIRMS.")
        sys.exit(1)
    csvs = list(PASTA_INPUT.glob('*.csv'))
    if not csvs:
        print(f"⚠️  Nenhum CSV encontrado em '{PASTA_INPUT}'.")
        sys.exit(1)
    return csvs

def carregar_firms(ficheiro, satelite):
    path = Path(ficheiro)
    if not path.exists():
        print(f'⚠️  Ficheiro não encontrado: {ficheiro}')
        return pd.DataFrame()
    df = pd.read_csv(ficheiro, low_memory=False)
    df['satelite'] = satelite
    if 'acq_date' in df.columns:
        df['acq_date'] = pd.to_datetime(df['acq_date'])
        df['ano']      = df['acq_date'].dt.year
        df['mes']      = df['acq_date'].dt.month
        df['dia']      = df['acq_date'].dt.day
    print(f'  ✅ {satelite}: {len(df):,} registos')
    return df

def atribuir_regiao(lat):
    if lat >= 41.0:              return 'Norte'
    elif lat >= 39.5:            return 'Centro'
    elif lat >= 38.5:            return 'Lisboa e VT'
    elif lat >= 37.5:            return 'Alentejo'
    else:                        return 'Algarve'

# ─────────────────────────────────────────────
# ANÁLISES E GRÁFICOS
# ─────────────────────────────────────────────
def analisar(df_pt):
    graficos = {}

    # 1. Hotspots por ano + FRP médio
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    por_ano = df_pt.groupby('ano').size().reset_index(name='hotspots')
    axes[0].bar(por_ano['ano'], por_ano['hotspots'], color='#e74c3c', alpha=0.85)
    axes[0].set_title('Hotspots por ano', fontsize=13, fontweight='bold')
    axes[0].set_xlabel('Ano'); axes[0].set_ylabel('Nº hotspots')
    axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f'{int(x):,}'))
    for b, v in zip(axes[0].patches, por_ano['hotspots']):
        axes[0].text(b.get_x()+b.get_width()/2, b.get_height()+2, f'{v:,}',
                     ha='center', va='bottom', fontsize=9)
    if 'frp' in df_pt.columns:
        frp_ano = df_pt.groupby('ano')['frp'].mean()
        axes[1].plot(frp_ano.index, frp_ano.values, 'o-', color='#e67e22', linewidth=2, markersize=8)
        axes[1].set_title('FRP médio por ano (intensidade)', fontsize=13, fontweight='bold')
        axes[1].set_xlabel('Ano'); axes[1].set_ylabel('FRP médio (MW)')
        axes[1].fill_between(frp_ano.index, frp_ano.values, alpha=0.1, color='#e67e22')
    plt.tight_layout()
    graficos['hotspots_ano'] = fig_para_base64()

    # 2. Sazonalidade por mês
    por_mes = df_pt.groupby('mes').size().reindex(range(1,13), fill_value=0)
    cores = ['#3498db']*5 + ['#e74c3c']*4 + ['#3498db']*3
    fig, ax = plt.subplots(figsize=(12, 5))
    bars = ax.bar(range(1,13), por_mes.values, color=cores, edgecolor='white', linewidth=0.5)
    ax.set_xticks(range(1,13)); ax.set_xticklabels(MESES_PT)
    ax.set_title('Sazonalidade — Hotspots por mês', fontsize=13, fontweight='bold')
    ax.set_ylabel('Nº hotspots')
    ax.axvspan(5.5, 9.5, alpha=0.07, color='red', label='Época de risco alto')
    ax.legend()
    for b, v in zip(bars, por_mes.values):
        if v > 0:
            ax.text(b.get_x()+b.get_width()/2, b.get_height()+1, f'{v:,}',
                    ha='center', va='bottom', fontsize=8)
    plt.tight_layout()
    graficos['sazonalidade'] = fig_para_base64()

    # 3. Heatmap ano × mês
    hm = df_pt.groupby(['ano','mes']).size().unstack(fill_value=0)
    hm.columns = [MESES_PT[c-1] for c in hm.columns]
    fig, ax = plt.subplots(figsize=(14, max(3, len(hm)*0.8+1)))
    sns.heatmap(hm, annot=True, fmt=',d', cmap='YlOrRd', linewidths=0.5,
                ax=ax, cbar_kws={'label': 'Nº hotspots'})
    ax.set_title('Heatmap — Ano × Mês', fontsize=13, fontweight='bold')
    plt.tight_layout()
    graficos['heatmap'] = fig_para_base64()

    # 4. Mapa de densidade
    fig, axes = plt.subplots(1, 2, figsize=(14, 8))
    frp_col = df_pt['frp'] if 'frp' in df_pt.columns else None
    sc = axes[0].scatter(df_pt['longitude'], df_pt['latitude'],
                         c=frp_col if frp_col is not None else 'red',
                         cmap='hot_r', alpha=0.3, s=2,
                         vmin=0, vmax=frp_col.quantile(0.95) if frp_col is not None else 100)
    if frp_col is not None:
        plt.colorbar(sc, ax=axes[0], label='FRP (MW)')
    axes[0].set_title('Densidade de hotspots\n(cor = intensidade FRP)', fontsize=12, fontweight='bold')
    axes[0].set_xlim(-9.6,-6.1); axes[0].set_ylim(36.8,42.3)
    axes[0].set_xlabel('Longitude'); axes[0].set_ylabel('Latitude')

    df_pt['_epoca'] = df_pt['mes'].apply(lambda m: 'Verão (Jun–Set)' if m in [6,7,8,9] else 'Resto do ano')
    for ep, grp in df_pt.groupby('_epoca'):
        cor = '#e74c3c' if 'Verão' in ep else '#3498db'
        axes[1].scatter(grp['longitude'], grp['latitude'], c=cor, alpha=0.2, s=2, label=ep)
    axes[1].set_title('Hotspots por época do ano', fontsize=12, fontweight='bold')
    axes[1].set_xlim(-9.6,-6.1); axes[1].set_ylim(36.8,42.3)
    axes[1].legend(markerscale=5)
    axes[1].set_xlabel('Longitude'); axes[1].set_ylabel('Latitude')
    plt.tight_layout()
    graficos['mapa'] = fig_para_base64()
    df_pt.drop(columns=['_epoca'], inplace=True)

    # 5. Regiões
    df_pt['_regiao'] = df_pt['latitude'].apply(atribuir_regiao)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    por_regiao = df_pt['_regiao'].value_counts()
    por_regiao.plot(kind='barh', ax=axes[0],
                    color=sns.color_palette('Reds_r', len(por_regiao)))
    axes[0].set_title('Hotspots por região', fontsize=13, fontweight='bold')
    axes[0].set_xlabel('Nº hotspots')
    if 'frp' in df_pt.columns:
        frp_reg = df_pt.groupby('_regiao')['frp'].mean().sort_values()
        frp_reg.plot(kind='barh', ax=axes[1],
                     color=sns.color_palette('Oranges_r', len(frp_reg)))
        axes[1].set_title('FRP médio por região', fontsize=13, fontweight='bold')
        axes[1].set_xlabel('FRP médio (MW)')
    plt.tight_layout()
    graficos['regioes'] = fig_para_base64()

    # 6. Análise FRP
    if 'frp' in df_pt.columns:
        fig, axes = plt.subplots(1, 3, figsize=(16, 5))
        frp_clip = df_pt['frp'].clip(upper=df_pt['frp'].quantile(0.99))
        axes[0].hist(frp_clip, bins=60, color='#e74c3c', alpha=0.8, edgecolor='white')
        axes[0].axvline(df_pt['frp'].median(), color='navy', linestyle='--',
                        label=f'Mediana: {df_pt["frp"].median():.1f} MW')
        axes[0].set_title('Distribuição do FRP', fontsize=12, fontweight='bold')
        axes[0].set_xlabel('FRP (MW)'); axes[0].set_ylabel('Frequência')
        axes[0].legend()

        meses_labels = {5:'Mai',6:'Jun',7:'Jul',8:'Ago',9:'Set',10:'Out'}
        df_v = df_pt[df_pt['mes'].isin(meses_labels)].copy()
        df_v['mes_l'] = df_v['mes'].map(meses_labels)
        order = ['Mai','Jun','Jul','Ago','Set','Out']
        sns.boxplot(data=df_v, x='mes_l', y='frp', order=order,
                    palette='YlOrRd', ax=axes[1],
                    flierprops=dict(marker='o', markersize=2, alpha=0.3))
        axes[1].set_title('FRP por mês (época de risco)', fontsize=12, fontweight='bold')
        axes[1].set_xlabel('Mês'); axes[1].set_ylabel('FRP (MW)')
        axes[1].set_ylim(0, df_pt['frp'].quantile(0.95))

        if 'daynight' in df_pt.columns:
            dn = df_pt.copy()
            dn['periodo'] = dn['daynight'].map({'D':'Diurno','N':'Noturno'})
            sns.boxplot(data=dn, x='periodo', y='frp',
                        palette=['#f39c12','#2c3e50'], ax=axes[2],
                        flierprops=dict(marker='o', markersize=2, alpha=0.3))
            axes[2].set_title('FRP: Diurno vs Noturno', fontsize=12, fontweight='bold')
            axes[2].set_ylabel('FRP (MW)')
            axes[2].set_ylim(0, df_pt['frp'].quantile(0.95))
        plt.tight_layout()
        graficos['frp'] = fig_para_base64()

    # 7. Comparação satélites
    if df_pt['satelite'].nunique() > 1:
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        contagem = df_pt['satelite'].value_counts()
        axes[0].pie(contagem.values, labels=contagem.index,
                    autopct='%1.1f%%', colors=['#3498db','#e74c3c'],
                    startangle=90, wedgeprops=dict(edgecolor='white', linewidth=2))
        axes[0].set_title('Distribuição por satélite', fontsize=12, fontweight='bold')
        if 'frp' in df_pt.columns:
            frp_sat = df_pt.groupby(['ano','satelite'])['frp'].mean().unstack()
            frp_sat.plot(ax=axes[1], marker='o', linewidth=2)
            axes[1].set_title('FRP médio por ano e satélite', fontsize=12, fontweight='bold')
            axes[1].set_xlabel('Ano'); axes[1].set_ylabel('FRP médio (MW)')
            axes[1].legend(title='Satélite')
        plt.tight_layout()
        graficos['satelites'] = fig_para_base64()

    # 8. Correlações
    cols_num = [c for c in ['latitude','longitude','bright_ti4','bright_ti5','frp','mes','ano']
                if c in df_pt.columns]
    if len(cols_num) >= 3:
        corr = df_pt[cols_num].corr()
        fig, ax = plt.subplots(figsize=(9, 7))
        mask = np.triu(np.ones_like(corr, dtype=bool))
        sns.heatmap(corr, mask=mask, annot=True, fmt='.2f', cmap='RdBu_r',
                    center=0, vmin=-1, vmax=1, ax=ax, linewidths=0.5,
                    cbar_kws={'label': 'Correlação de Pearson'})
        ax.set_title('Matriz de correlação entre variáveis', fontsize=13, fontweight='bold')
        plt.tight_layout()
        graficos['correlacoes'] = fig_para_base64()

    df_pt.drop(columns=['_regiao'], inplace=True, errors='ignore')
    return graficos

# ─────────────────────────────────────────────
# CALCULAR MÉTRICAS PARA O RESUMO
# ─────────────────────────────────────────────
def calcular_metricas(df, df_pt, fora_bbox):
    m = {}
    m['total_raw']      = len(df)
    m['total_pt']       = len(df_pt)
    m['fora_bbox']      = len(fora_bbox)
    m['periodo_ini']    = df_pt['acq_date'].min().date()
    m['periodo_fim']    = df_pt['acq_date'].max().date()
    m['anos']           = sorted(df_pt['ano'].unique())
    m['colunas']        = df_pt.shape[1]
    nulos_col           = df_pt.isnull().sum()
    m['nulos']          = int(nulos_col.sum())
    m['nulos_por_coluna'] = nulos_col.to_dict()
    m['satelites']      = list(df_pt['satelite'].unique())

    # Duplicados
    m['duplicados']     = int(df_pt.duplicated().sum())
    m['duplicados_pct'] = round(m['duplicados'] / len(df_pt) * 100, 2) if len(df_pt) > 0 else 0

    if 'frp' in df_pt.columns:
        m['frp_mediana'] = round(df_pt['frp'].median(), 1)
        m['frp_media']   = round(df_pt['frp'].mean(), 1)
        m['frp_max']     = round(df_pt['frp'].max(), 1)
        q3  = df_pt['frp'].quantile(0.75)
        iqr = q3 - df_pt['frp'].quantile(0.25)
        m['frp_outliers'] = int((df_pt['frp'] > q3 + 3*iqr).sum())

    por_mes = df_pt.groupby('mes').size()
    m['mes_pico']    = MESES_PT[por_mes.idxmax()-1]
    m['pct_verao']   = round(por_mes[6:10].sum() / len(df_pt) * 100, 1)

    df_pt['_r'] = df_pt['latitude'].apply(atribuir_regiao)
    m['regiao_top'] = df_pt['_r'].value_counts().idxmax()
    df_pt.drop(columns=['_r'], inplace=True)

    return m

# ─────────────────────────────────────────────
# SCHEMA TABLE
# ─────────────────────────────────────────────
SCHEMA = [
    ('latitude',    'float64',       '✅ Sim',         'Localização geográfica do hotspot'),
    ('longitude',   'float64',       '✅ Sim',         'Localização geográfica do hotspot'),
    ('bright_ti4',  'float64',       '✅ Sim',         'Temperatura de brilho banda I4 — proxy de intensidade'),
    ('bright_ti5',  'float64',       '✅ Sim',         'Temperatura de brilho banda I5 — temperatura ambiente'),
    ('frp',         'float64',       '✅ Sim — principal', 'Fire Radiative Power (MW): intensidade do fogo'),
    ('acq_date',    'datetime64',    '✅ Transformar', 'Extrair mês, dia do ano, estação do ano'),
    ('acq_time',    'int64',         '✅ Transformar', 'Converter para hora do dia (0–23)'),
    ('confidence',  'object',        '✅ Filtrar',     'Manter só "nominal" e "high"'),
    ('daynight',    'object',        '✅ Sim',         'Feature binária: D = diurno, N = noturno'),
    ('satellite',   'object',        '⚠️ Opcional',   'Distinguir S-NPP vs NOAA-20'),
    ('scan',        'float64',       '❌ Não',         'Tamanho do píxel — metadado técnico'),
    ('track',       'float64',       '❌ Não',         'Tamanho do píxel — metadado técnico'),
    ('instrument',  'object',        '❌ Não',         'Sempre "VIIRS" — sem variação'),
    ('version',     'int64',         '❌ Não',         'Versão do algoritmo — metadado'),
    ('type',        'int64',         '❌ Não',         'Sempre 0 (presumed vegetation fire)'),
    ('ano',         'int32 (derivada)',  '✅ Sim',     'Extraído de acq_date'),
    ('mes',         'int32 (derivada)',  '✅ Sim',     'Sazonalidade — feature importante'),
    ('dia',         'int32 (derivada)',  '✅ Sim',     'Dia do mês'),
]

# ─────────────────────────────────────────────
# GERAR HTML
# ─────────────────────────────────────────────
def gerar_html(m, graficos, descritivas_html, ficheiros_input):
    now = datetime.now().strftime('%d/%m/%Y %H:%M')

    schema_rows = ''
    for var, tipo, rel, motivo in SCHEMA:
        cor = '#d4edda' if '✅' in rel else ('#fff3cd' if '⚠️' in rel else '#f8d7da')
        schema_rows += f'<tr style="background:{cor}"><td><code>{var}</code></td><td>{tipo}</td><td>{rel}</td><td>{motivo}</td></tr>'

    graficos_html = ''
    titulos = {
        'hotspots_ano':  'Hotspots por ano e FRP médio',
        'sazonalidade':  'Sazonalidade — distribuição mensal',
        'heatmap':       'Heatmap ano × mês',
        'mapa':          'Mapa de densidade geográfica',
        'regioes':       'Análise por região',
        'frp':           'Análise do FRP (Fire Radiative Power)',
        'satelites':     'Comparação entre satélites',
        'correlacoes':   'Matriz de correlação',
    }
    for key, titulo in titulos.items():
        if key in graficos:
            graficos_html += img_html(graficos[key], titulo)

    anos_str = ', '.join(map(str, m['anos']))
    sats_str = ', '.join(m['satelites'])
    ficheiros_str = '<br>'.join(f'<code>{f}</code>' for f in ficheiros_input)

    # Tabela de nulls por coluna
    nulos_rows = ''
    for col, n in m['nulos_por_coluna'].items():
        pct  = round(n / m['total_pt'] * 100, 2) if m['total_pt'] > 0 else 0
        cor  = '#f8d7da' if n > 0 else '#d4edda'
        icon = '⚠️' if n > 0 else '✅'
        nulos_rows += f'<tr style="background:{cor}"><td><code>{col}</code></td><td>{n:,}</td><td>{pct}%</td><td>{icon}</td></tr>'
    nulos_tabela_html = f'''
    <table class="tabela">
      <tr><th>Coluna</th><th>Nulls</th><th>% do total</th><th>Estado</th></tr>
      {nulos_rows if nulos_rows else '<tr><td colspan="4" style="text-align:center;color:#27ae60">✅ Nenhuma coluna com valores em falta</td></tr>'}
    </table>'''

    html = f'''<!DOCTYPE html>
<html lang="pt">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ForestRisk — Relatório EDA</title>
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; background: #f5f5f5; color: #222; }}
  .header {{ background: linear-gradient(135deg, #c0392b, #e74c3c); color: white; padding: 36px 48px; }}
  .header h1 {{ margin: 0 0 6px; font-size: 28px; }}
  .header p {{ margin: 0; opacity: .85; font-size: 14px; }}
  .container {{ max-width: 1100px; margin: 0 auto; padding: 32px 24px; }}
  h2 {{ font-size: 20px; color: #c0392b; border-left: 4px solid #e74c3c; padding-left: 12px; margin-top: 40px; }}
  h3 {{ font-size: 16px; color: #444; margin-top: 28px; }}
  .cards {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 20px 0; }}
  .card {{ background: white; border-radius: 10px; padding: 20px 24px; flex: 1; min-width: 160px;
           box-shadow: 0 1px 4px rgba(0,0,0,.08); border-top: 3px solid #e74c3c; }}
  .card .valor {{ font-size: 28px; font-weight: 700; color: #c0392b; }}
  .card .label {{ font-size: 12px; color: #888; margin-top: 4px; }}
  .grafico {{ background: white; border-radius: 10px; padding: 20px;
              box-shadow: 0 1px 4px rgba(0,0,0,.08); margin: 16px 0; }}
  .grafico-titulo {{ font-weight: 600; color: #444; margin: 0 0 12px; font-size: 14px; }}
  .grafico img {{ width: 100%; height: auto; border-radius: 6px; }}
  table.tabela {{ border-collapse: collapse; width: 100%; font-size: 13px; margin: 16px 0; }}
  table.tabela th {{ background: #c0392b; color: white; padding: 10px 12px; text-align: left; }}
  table.tabela td {{ padding: 8px 12px; border-bottom: 1px solid #eee; }}
  table.tabela tr:hover {{ background: #fafafa; }}
  code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 4px; font-size: 12px; }}
  .resumo-box {{ background: white; border-radius: 10px; padding: 24px;
                 box-shadow: 0 1px 4px rgba(0,0,0,.08); margin: 16px 0; }}
  .resumo-box ul {{ margin: 0; padding-left: 20px; line-height: 1.9; }}
  .badge-sim  {{ background:#d4edda; color:#155724; padding:2px 8px; border-radius:99px; font-size:11px; }}
  .badge-nao  {{ background:#f8d7da; color:#721c24; padding:2px 8px; border-radius:99px; font-size:11px; }}
  .badge-opt  {{ background:#fff3cd; color:#856404; padding:2px 8px; border-radius:99px; font-size:11px; }}
  .footer {{ text-align:center; color:#aaa; font-size:12px; padding:32px 0; }}
  .alerta {{ background:#fff3cd; border-left:4px solid #ffc107; padding:12px 16px;
             border-radius:0 8px 8px 0; margin:16px 0; font-size:13px; }}
</style>
</head>
<body>

<div class="header">
  <h1>🔥 ForestRisk — Relatório EDA</h1>
  <p>Análise Exploratória de Dados · NASA FIRMS · ISEP Pós-Graduação Big Data &amp; Data Science 2024/2025</p>
  <p style="margin-top:8px;opacity:.7">Pessoa B — Data Scientist · Gerado em {now}</p>
</div>

<div class="container">

  <h2>Ficheiros analisados</h2>
  <div class="resumo-box">{ficheiros_str}</div>

  <h2>Resumo executivo</h2>
  <div class="cards">
    <div class="card"><div class="valor">{m['total_pt']:,}</div><div class="label">Hotspots (Portugal)</div></div>
    <div class="card"><div class="valor">{m['nulos']}</div><div class="label">Valores em falta (total)</div></div>
    <div class="card" style="border-top-color:{'#e74c3c' if m['duplicados']>0 else '#27ae60'}">
      <div class="valor" style="color:{'#c0392b' if m['duplicados']>0 else '#27ae60'}">{m['duplicados']:,}</div>
      <div class="label">Duplicados</div>
    </div>
    <div class="card"><div class="valor">{m.get('frp_mediana','—')} MW</div><div class="label">FRP mediano</div></div>
    <div class="card"><div class="valor">{m.get('frp_max','—')} MW</div><div class="label">FRP máximo</div></div>
    <div class="card"><div class="valor">{m['pct_verao']}%</div><div class="label">Jul–Out no total</div></div>
    <div class="card"><div class="valor">{m['mes_pico']}</div><div class="label">Mês com mais ocorrências</div></div>
  </div>

  <div class="resumo-box">
    <ul>
      <li><strong>Período:</strong> {m['periodo_ini']} → {m['periodo_fim']}</li>
      <li><strong>Anos cobertos:</strong> {anos_str}</li>
      <li><strong>Satélites:</strong> {sats_str}</li>
      <li><strong>Registos fora do bounding box PT:</strong> {m['fora_bbox']:,} ({round(m['fora_bbox']/m['total_raw']*100,2)}%)</li>
      <li><strong>Região mais afectada:</strong> {m['regiao_top']}</li>
      <li><strong>FRP médio:</strong> {m.get('frp_media','—')} MW · mediana {m.get('frp_mediana','—')} MW · máximo {m.get('frp_max','—')} MW</li>
      <li><strong>Outliers extremos FRP</strong> (&gt; Q3 + 3×IQR): {m.get('frp_outliers','—')}</li>
      <li><strong>Sazonalidade:</strong> {m['pct_verao']}% dos hotspots ocorrem entre julho e outubro</li>
    </ul>
  </div>

  <h2>Qualidade dos dados</h2>

  <h3>Valores em falta por coluna</h3>
  {nulos_tabela_html}

  <h3>Duplicados</h3>
  <div class="resumo-box">
    <ul>
      <li><strong>Linhas duplicadas exactas:</strong> {m['duplicados']:,} ({m['duplicados_pct']}% do total)</li>
      <li><strong>Conclusão:</strong> {"⚠️ Existem duplicados — serão removidos antes do Feature Engineering" if m['duplicados'] > 0 else "✅ Sem duplicados — dataset íntegro"}</li>
    </ul>
  </div>

  <h2>Schema de dados e variáveis relevantes</h2>
  <p style="font-size:13px;color:#666">Definição do schema com base na análise exploratória — identifica quais as variáveis a usar no pipeline ML (Feature Engineering → Random Forest + XGBoost).</p>
  <table class="tabela">
    <tr><th>Variável</th><th>Tipo</th><th>Para ML?</th><th>Motivo</th></tr>
    {schema_rows}
  </table>

  <h2>Estatísticas descritivas</h2>
  {descritivas_html}

  <h2>Visualizações</h2>
  {graficos_html}

 
</div>
<div class="footer">ForestRisk · ISEP 2024/2025 · Pessoa B — Data Scientist · {now}</div>
</body>
</html>'''
    return html

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print('\n🔥 ForestRisk — EDA Automático')
    print('=' * 45)

    # Detectar ficheiros
    if len(sys.argv) > 1:
        ficheiros = sys.argv[1:]
    else:
        ficheiros = [str(p) for p in detectar_csvs()]

    print(f'\n📂 Ficheiros encontrados:')
    for f in ficheiros:
        print(f'   {f}')

    # Carregar
    print('\n📥 A carregar dados...')
    frames = []
    for i, f in enumerate(ficheiros):
        sat = f'Satélite {i+1}' if len(ficheiros) > 1 else 'VIIRS'
        if 'snpp' in f.lower() or 's-npp' in f.lower():
            sat = 'VIIRS S-NPP'
        elif 'noaa' in f.lower() or 'jpss1' in f.lower() or 'jpss2' in f.lower():
            sat = 'VIIRS NOAA-20'
        df_tmp = carregar_firms(f, sat)
        if not df_tmp.empty:
            frames.append(df_tmp)

    if not frames:
        print('❌ Nenhum dado carregado. Verifica os ficheiros.')
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True)
    print(f'\n  📊 Total combinado: {len(df):,} hotspots')

    # Filtrar Portugal
    fora = df[
        (df['latitude']  < LAT_MIN) | (df['latitude']  > LAT_MAX) |
        (df['longitude'] < LON_MIN) | (df['longitude'] > LON_MAX)
    ]
    df_pt = df[
        (df['latitude']  >= LAT_MIN) & (df['latitude']  <= LAT_MAX) &
        (df['longitude'] >= LON_MIN) & (df['longitude'] <= LON_MAX)
    ].copy()
    print(f'  🗺️  Filtrados para Portugal: {len(df_pt):,} hotspots ({len(fora):,} fora do bbox)')

    # Métricas
    print('\n📈 A calcular métricas...')
    m = calcular_metricas(df, df_pt, fora)

    # Estatísticas descritivas
    cols_desc = [c for c in ['latitude','longitude','bright_ti4','bright_ti5','frp'] if c in df_pt.columns]
    descritivas_html = df_pt[cols_desc].describe().round(2).to_html(
        classes='tabela', border=0)

    # Gráficos
    print('📊 A gerar gráficos (pode demorar alguns segundos)...')
    graficos = analisar(df_pt)
    print(f'  ✅ {len(graficos)} gráficos gerados')

    # HTML
    print('\n📝 A gerar relatório HTML...')
    html = gerar_html(m, graficos, descritivas_html, ficheiros)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)

    # Colunas a guardar — apenas as originais + derivadas úteis
    COLUNAS_GUARDAR = [
        'latitude', 'longitude', 'bright_ti4', 'bright_ti5', 'frp',
        'acq_date', 'acq_time', 'confidence', 'daynight', 'satellite',
        'satelite', 'ano', 'mes', 'dia'
    ]
    cols_presentes = [c for c in COLUNAS_GUARDAR if c in df_pt.columns]
    df_guardar = df_pt[cols_presentes].copy()

    # Guardar CSV + Parquet por ano nas pastas correctas
    PASTA_CSV.mkdir(exist_ok=True)
    PASTA_PARQUET.mkdir(exist_ok=True)

    print('\n💾 A guardar ficheiros por ano...')
    anos_guardados = []
    for ano, df_ano in df_guardar.groupby('ano'):
        nome_csv     = PASTA_CSV     / f'firms_portugal_limpo_{ano}.csv'
        nome_parquet = PASTA_PARQUET / f'firms_portugal_limpo_{ano}.parquet'

        df_ano.to_csv(nome_csv, index=False)

        try:
            df_ano.to_parquet(nome_parquet, index=False)
            print(f'  ✅ {ano}: {len(df_ano):,} registos → {nome_csv.name} + {nome_parquet.name}')
        except Exception:
            print(f'  ✅ {ano}: {len(df_ano):,} registos → {nome_csv.name} (parquet ignorado)')

        anos_guardados.append(str(ano))

    # Guardar também ficheiro combinado de todos os anos
    df_guardar.to_csv(PASTA_CSV / 'firms_portugal_limpo_todos.csv', index=False)
    try:
        df_guardar.to_parquet(PASTA_PARQUET / 'firms_portugal_limpo_todos.parquet', index=False)
    except Exception:
        pass

    print(f'\n✅ Relatório gerado        : {OUTPUT_HTML}')
    print(f'✅ Ficheiros por ano       : firms_portugal_limpo_[{", ".join(anos_guardados)}].csv/.parquet')
    print(f'✅ Ficheiro combinado      : firms_portugal_limpo_todos.csv/.parquet')
    print(f'\n   Abre o ficheiro {OUTPUT_HTML} no browser para ver o relatório completo.')
    print('=' * 45)

if __name__ == '__main__':
    main()
