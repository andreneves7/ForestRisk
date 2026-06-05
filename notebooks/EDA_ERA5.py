"""
ForestRisk — ERA5 EDA Automático com Relatório HTML
====================================================
Lê dados ERA5 do Copernicus (NetCDF/ZIP), filtra Portugal e gera relatório EDA.

Uso:
    python era5_eda_relatorio.py

Estrutura de pastas:
    ERA5/               ← coloca aqui o .zip ou .nc do ERA5
    ERA5_CSV/           ← CSVs processados por ano (criada automaticamente)
    ERA5_Parquet/       ← Parquet processados por ano (criada automaticamente)
    era5_eda_relatorio.html  ← relatório HTML gerado

Dependências:
    pip install xarray netcdf4 h5netcdf scipy
"""

import sys
import zipfile
import base64
import io
import warnings
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
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
LAT_MIN, LAT_MAX  = 36.9, 42.2
LON_MIN, LON_MAX  = -9.5, -6.2
MESES_PT          = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']
PASTA_INPUT       = Path('ERA5')
PASTA_CSV         = Path('ERA5_CSV')
PASTA_PARQUET     = Path('ERA5_Parquet')
OUTPUT_HTML       = 'era5_eda_relatorio.html'

# Limiares de risco ForestRisk (do documento do projecto)
TEMP_RISCO        = 35.0   # °C
HUM_RISCO         = 20.0   # %
VENTO_RISCO       = 30.0   # km/h

# ─────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────
def fig_para_base64():
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=120, bbox_inches='tight')
    plt.close()
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')

def img_html(b64, titulo=''):
    return f'''
    <div class="grafico">
        <p class="grafico-titulo">{titulo}</p>
        <img src="data:image/png;base64,{b64}" alt="{titulo}">
    </div>'''

# ─────────────────────────────────────────────
# CARREGAR DADOS
# ─────────────────────────────────────────────
def encontrar_nc():
    """Encontra ficheiros .nc na pasta ERA5, extraindo ZIP se necessário."""
    if not PASTA_INPUT.exists():
        print(f"⚠️  Pasta '{PASTA_INPUT}' não encontrada.")
        print("   Cria a pasta ERA5/ e coloca lá o ficheiro ZIP do Copernicus.")
        sys.exit(1)

    # Se já existem NC extraídos, usa-os directamente
    nc_files = list(PASTA_INPUT.glob('*.nc'))
    if nc_files:
        print(f'  📂 {len(nc_files)} ficheiro(s) NetCDF encontrado(s):')
        for f in nc_files:
            print(f'     {f.name}')
        return nc_files

    # Procurar ZIP para extrair
    zip_files = list(PASTA_INPUT.glob('*.zip'))
    if not zip_files:
        print("⚠️  Nenhum ficheiro .zip ou .nc encontrado em ERA5/")
        sys.exit(1)

    print(f'  📦 ZIP encontrado: {zip_files[0].name}')
    print(f'  📤 A extrair... (pode demorar alguns minutos para 22 GB)')
    with zipfile.ZipFile(zip_files[0], 'r') as z:
        nc_dentro = [f for f in z.namelist() if f.endswith('.nc')]
        print(f'     Ficheiros dentro do ZIP: {nc_dentro}')
        z.extractall(PASTA_INPUT)
    print(f'  ✅ Extracção concluída')
    return list(PASTA_INPUT.glob('*.nc'))


def carregar_era5(nc_files):
    """Carrega NetCDF com xarray e filtra para Portugal Continental."""
    import xarray as xr

    print(f'  📖 A carregar {len(nc_files)} ficheiro(s)...')
    if len(nc_files) == 1:
        ds = xr.open_dataset(nc_files[0], engine='netcdf4')
    else:
        ds = xr.open_mfdataset(nc_files, combine='by_coords', engine='netcdf4')

    # Renomear 'valid_time' para 'time' se necessário (versões recentes ERA5)
    if 'valid_time' in ds.dims and 'time' not in ds.dims:
        ds = ds.rename({'valid_time': 'time'})

    print(f'  📊 Variáveis  : {list(ds.data_vars)}')
    print(f'  📅 Período    : {str(ds.time.values[0])[:10]} → {str(ds.time.values[-1])[:10]}')
    print(f'  🗺️  Grid global: lat {float(ds.latitude.min()):.1f}–{float(ds.latitude.max()):.1f}, '
          f'lon {float(ds.longitude.min()):.1f}–{float(ds.longitude.max()):.1f}')

    # Corrigir longitude 0-360 → -180/180 se necessário
    if float(ds.longitude.max()) > 180:
        print('  🔄 A converter longitude de 0-360 para -180/180...')
        new_lon = ((ds.longitude.values + 180) % 360) - 180
        ds = ds.assign_coords(longitude=new_lon)
        ds = ds.sortby('longitude')

    # Filtrar para Portugal (ERA5 latitude é decrescente: 90→-90)
    lat_desc = float(ds.latitude.values[0]) > float(ds.latitude.values[-1])
    lat_slice = slice(LAT_MAX, LAT_MIN) if lat_desc else slice(LAT_MIN, LAT_MAX)
    ds_pt = ds.sel(latitude=lat_slice, longitude=slice(LON_MIN, LON_MAX))

    n_lat  = len(ds_pt.latitude)
    n_lon  = len(ds_pt.longitude)
    n_time = len(ds_pt.time)
    print(f'  🇵🇹 Portugal  : {n_lat}×{n_lon} pontos de grid · {n_time:,} instantes de tempo')
    return ds_pt


def calcular_derivadas(df):
    """Calcula variáveis meteorológicas derivadas relevantes para risco de incêndio."""

    # Temperatura K → °C
    if 't2m' in df.columns:
        df['temp_c'] = df['t2m'] - 273.15
    if 'd2m' in df.columns:
        df['dewpoint_c'] = df['d2m'] - 273.15

    # Velocidade do vento: componentes u,v → km/h
    if 'u10' in df.columns and 'v10' in df.columns:
        df['wind_speed_kmh'] = np.sqrt(df['u10']**2 + df['v10']**2) * 3.6

    # Humidade relativa (%) — fórmula de Magnus
    if 'temp_c' in df.columns and 'dewpoint_c' in df.columns:
        T, Td = df['temp_c'], df['dewpoint_c']
        df['rh'] = (100 * np.exp((17.625 * Td) / (243.04 + Td)) /
                         np.exp((17.625 * T)  / (243.04 + T))).clip(0, 100)

    # Precipitação: kg/m²/s → mm/dia
    if 'mtpr' in df.columns:
        df['precip_mm_dia'] = df['mtpr'].clip(lower=0) * 86400

    # Evaporação: kg/m²/s → mm/dia (valor absoluto)
    if 'mer' in df.columns:
        df['evap_mm_dia'] = df['mer'].abs() * 86400

    # Colunas temporais
    if 'time' in df.columns:
        dt = pd.to_datetime(df['time'])
        df['ano']  = dt.dt.year
        df['mes']  = dt.dt.month
        df['dia']  = dt.dt.day
        df['hora'] = dt.dt.hour

    # Flag risco alto (T > 35°C E HR < 20% E Vento > 30 km/h)
    conds = []
    if 'temp_c'         in df.columns: conds.append(df['temp_c']         > TEMP_RISCO)
    if 'rh'             in df.columns: conds.append(df['rh']             < HUM_RISCO)
    if 'wind_speed_kmh' in df.columns: conds.append(df['wind_speed_kmh'] > VENTO_RISCO)
    if conds:
        flag = conds[0]
        for c in conds[1:]:
            flag = flag & c
        df['risco_alto'] = flag

    return df

# ─────────────────────────────────────────────
# ANÁLISES E GRÁFICOS
# ─────────────────────────────────────────────
def analisar(df):
    graficos = {}

    # Agregar por instante de tempo (média sobre todos os pontos de Portugal)
    agg = {k: 'mean' for k in ['temp_c','dewpoint_c','rh','wind_speed_kmh',
                                 'precip_mm_dia','evap_mm_dia'] if k in df.columns}
    if 'risco_alto' in df.columns:
        agg['risco_alto'] = 'any'
    for c in ['mes','ano','dia','hora']:
        if c in df.columns:
            agg[c] = 'first'

    df_t = df.groupby('time').agg(agg).reset_index()

    # ── 1. Temperatura ───────────────────────
    if 'temp_c' in df_t.columns:
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        pm = df_t.groupby('mes')['temp_c'].agg(['mean','max','min'])
        x  = range(1, 13)
        axes[0].fill_between(x, pm['min'], pm['max'], alpha=0.2, color='#e74c3c', label='Min–Max')
        axes[0].plot(x, pm['mean'], 'o-', color='#e74c3c', linewidth=2, markersize=6, label='Média')
        axes[0].axhline(TEMP_RISCO, color='red', linestyle='--', alpha=0.6,
                        label=f'Limiar risco ({TEMP_RISCO}°C)')
        axes[0].set_xticks(x); axes[0].set_xticklabels(MESES_PT)
        axes[0].set_title('Temperatura 2m — média mensal Portugal', fontsize=12, fontweight='bold')
        axes[0].set_ylabel('Temperatura (°C)'); axes[0].legend(fontsize=9)

        sns.boxplot(data=df_t, x='mes', y='temp_c', palette='YlOrRd', ax=axes[1],
                    flierprops=dict(marker='o', markersize=2, alpha=0.3))
        axes[1].set_xticklabels(MESES_PT)
        axes[1].axhline(TEMP_RISCO, color='red', linestyle='--', alpha=0.5)
        axes[1].set_title('Distribuição de temperatura por mês', fontsize=12, fontweight='bold')
        axes[1].set_xlabel('Mês'); axes[1].set_ylabel('Temperatura (°C)')
        plt.tight_layout()
        graficos['temperatura'] = fig_para_base64()

    # ── 2. Humidade relativa ─────────────────
    if 'rh' in df_t.columns:
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        pm_rh = df_t.groupby('mes')['rh'].mean()
        axes[0].bar(range(1,13), pm_rh.reindex(range(1,13), fill_value=0).values,
                    color='#3498db', alpha=0.85, edgecolor='white')
        axes[0].axhline(HUM_RISCO, color='red', linestyle='--',
                        label=f'Limiar risco ({HUM_RISCO}%)')
        axes[0].set_xticks(range(1,13)); axes[0].set_xticklabels(MESES_PT)
        axes[0].set_title('Humidade relativa média mensal', fontsize=12, fontweight='bold')
        axes[0].set_ylabel('HR (%)'); axes[0].legend(fontsize=9)

        seco = df_t[df_t['rh'] < HUM_RISCO].groupby('mes').size().reindex(range(1,13), fill_value=0)
        axes[1].bar(range(1,13), seco.values, color='#e67e22', alpha=0.85, edgecolor='white')
        axes[1].set_xticks(range(1,13)); axes[1].set_xticklabels(MESES_PT)
        axes[1].set_title(f'Obs. com HR < {HUM_RISCO}% por mês', fontsize=12, fontweight='bold')
        axes[1].set_ylabel('Nº observações')
        plt.tight_layout()
        graficos['humidade'] = fig_para_base64()

    # ── 3. Vento ─────────────────────────────
    if 'wind_speed_kmh' in df_t.columns:
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        pm_v = df_t.groupby('mes')['wind_speed_kmh'].mean()
        axes[0].bar(range(1,13), pm_v.reindex(range(1,13), fill_value=0).values,
                    color='#2ecc71', alpha=0.85, edgecolor='white')
        axes[0].axhline(VENTO_RISCO, color='red', linestyle='--',
                        label=f'Limiar risco ({VENTO_RISCO} km/h)')
        axes[0].set_xticks(range(1,13)); axes[0].set_xticklabels(MESES_PT)
        axes[0].set_title('Velocidade do vento média mensal', fontsize=12, fontweight='bold')
        axes[0].set_ylabel('Velocidade (km/h)'); axes[0].legend(fontsize=9)

        sns.boxplot(data=df_t, x='mes', y='wind_speed_kmh', palette='Greens', ax=axes[1],
                    flierprops=dict(marker='o', markersize=2, alpha=0.3))
        axes[1].set_xticklabels(MESES_PT)
        axes[1].axhline(VENTO_RISCO, color='red', linestyle='--', alpha=0.5)
        axes[1].set_title('Distribuição do vento por mês', fontsize=12, fontweight='bold')
        axes[1].set_xlabel('Mês'); axes[1].set_ylabel('Velocidade (km/h)')
        plt.tight_layout()
        graficos['vento'] = fig_para_base64()

    # ── 4. Precipitação ──────────────────────
    if 'precip_mm_dia' in df_t.columns:
        fig, ax = plt.subplots(figsize=(12, 5))
        pp = df_t.groupby('mes')['precip_mm_dia'].sum().reindex(range(1,13), fill_value=0)
        bars = ax.bar(range(1,13), pp.values, color='#3498db', alpha=0.85, edgecolor='white')
        ax.set_xticks(range(1,13)); ax.set_xticklabels(MESES_PT)
        ax.set_title('Precipitação acumulada mensal — Portugal', fontsize=13, fontweight='bold')
        ax.set_ylabel('Precipitação (mm/dia acumulado)')
        for b, v in zip(bars, pp.values):
            if v > 0:
                ax.text(b.get_x()+b.get_width()/2, b.get_height()+0.05, f'{v:.1f}',
                        ha='center', va='bottom', fontsize=8)
        plt.tight_layout()
        graficos['precipitacao'] = fig_para_base64()

    # ── 5. Heatmap temperatura mês × hora ───
    if 'temp_c' in df_t.columns and 'hora' in df_t.columns:
        pivot = df_t.groupby(['mes','hora'])['temp_c'].mean().unstack()
        pivot.index = [MESES_PT[m-1] for m in pivot.index]
        fig, ax = plt.subplots(figsize=(14, 6))
        sns.heatmap(pivot, annot=True, fmt='.1f', cmap='YlOrRd',
                    ax=ax, linewidths=0.3, cbar_kws={'label': '°C'})
        ax.set_title('Temperatura média por mês e hora do dia (UTC)',
                     fontsize=13, fontweight='bold')
        ax.set_xlabel('Hora UTC'); ax.set_ylabel('Mês')
        plt.tight_layout()
        graficos['heatmap_temp'] = fig_para_base64()

    # ── 6. Dias de risco alto ────────────────
    if 'risco_alto' in df_t.columns:
        fig, ax = plt.subplots(figsize=(12, 5))
        rm = df_t[df_t['risco_alto']].groupby('mes').size().reindex(range(1,13), fill_value=0)
        cores = ['#e74c3c' if v > 0 else '#bdc3c7' for v in rm.values]
        bars = ax.bar(range(1,13), rm.values, color=cores, alpha=0.85, edgecolor='white')
        ax.set_xticks(range(1,13)); ax.set_xticklabels(MESES_PT)
        ax.set_title(
            f'Observações de risco alto por mês\n'
            f'(T > {TEMP_RISCO}°C  +  HR < {HUM_RISCO}%  +  Vento > {VENTO_RISCO} km/h)',
            fontsize=12, fontweight='bold')
        ax.set_ylabel('Nº observações')
        for b, v in zip(bars, rm.values):
            if v > 0:
                ax.text(b.get_x()+b.get_width()/2, b.get_height()+0.1, str(int(v)),
                        ha='center', va='bottom', fontsize=9)
        plt.tight_layout()
        graficos['risco'] = fig_para_base64()

    # ── 7. Correlações ───────────────────────
    cols_c = [c for c in ['temp_c','rh','wind_speed_kmh','precip_mm_dia','evap_mm_dia','mes']
              if c in df_t.columns]
    if len(cols_c) >= 3:
        corr = df_t[cols_c].corr()
        fig, ax = plt.subplots(figsize=(9, 7))
        mask = np.triu(np.ones_like(corr, dtype=bool))
        sns.heatmap(corr, mask=mask, annot=True, fmt='.2f', cmap='RdBu_r',
                    center=0, vmin=-1, vmax=1, ax=ax, linewidths=0.5,
                    cbar_kws={'label': 'Correlação de Pearson'})
        ax.set_title('Correlação entre variáveis meteorológicas',
                     fontsize=13, fontweight='bold')
        plt.tight_layout()
        graficos['correlacoes'] = fig_para_base64()

    # ── 8. Mapa espacial temperatura média ──
    if 'temp_c' in df.columns:
        t_esp = df.groupby(['latitude','longitude'])['temp_c'].mean().reset_index()
        pivot_m = t_esp.pivot(index='latitude', columns='longitude', values='temp_c')
        fig, ax = plt.subplots(figsize=(10, 8))
        im = ax.imshow(pivot_m.values, cmap='YlOrRd', aspect='auto',
                       extent=[LON_MIN, LON_MAX, LAT_MIN, LAT_MAX])
        plt.colorbar(im, ax=ax, label='Temperatura média (°C)')
        ax.set_title('Temperatura média anual — Portugal', fontsize=13, fontweight='bold')
        ax.set_xlabel('Longitude'); ax.set_ylabel('Latitude')
        plt.tight_layout()
        graficos['mapa_temp'] = fig_para_base64()

    return graficos

# ─────────────────────────────────────────────
# MÉTRICAS RESUMO
# ─────────────────────────────────────────────
def calcular_metricas(df):
    m = {}
    m['n_registos']  = len(df)
    m['n_pts_grid']  = df.groupby(['latitude','longitude']).ngroups
    m['periodo_ini'] = str(df['time'].min())[:10] if 'time' in df.columns else '—'
    m['periodo_fim'] = str(df['time'].max())[:10] if 'time' in df.columns else '—'
    m['anos']        = sorted(df['ano'].unique().tolist()) if 'ano' in df.columns else []
    m['nulos']       = int(df.isnull().sum().sum())
    m['nulos_col']   = df.isnull().sum().to_dict()
    m['duplicados']  = int(df.duplicated().sum())

    if 'temp_c'         in df.columns:
        m['temp_media']   = round(df['temp_c'].mean(), 1)
        m['temp_max']     = round(df['temp_c'].max(), 1)
        m['temp_min']     = round(df['temp_c'].min(), 1)
    if 'rh'             in df.columns:
        m['rh_media']     = round(df['rh'].mean(), 1)
    if 'wind_speed_kmh' in df.columns:
        m['vento_medio']  = round(df['wind_speed_kmh'].mean(), 1)
        m['vento_max']    = round(df['wind_speed_kmh'].max(), 1)
    if 'risco_alto'     in df.columns:
        m['obs_risco']    = int(df['risco_alto'].sum())
        m['pct_risco']    = round(m['obs_risco'] / max(len(df), 1) * 100, 2)
    return m

# ─────────────────────────────────────────────
# GERAR HTML
# ─────────────────────────────────────────────
def gerar_html(m, graficos, stats_html):
    now      = datetime.now().strftime('%d/%m/%Y %H:%M')
    anos_str = ', '.join(map(str, m.get('anos', [])))

    # Tabela nulls
    nulos_rows = ''
    skip = {'latitude','longitude','time','ano','mes','dia','hora'}
    for col, n in m.get('nulos_col', {}).items():
        if col in skip:
            continue
        pct  = round(n / max(m['n_registos'], 1) * 100, 2)
        cor  = '#f8d7da' if n > 0 else '#d4edda'
        icon = '⚠️' if n > 0 else '✅'
        nulos_rows += (f'<tr style="background:{cor}"><td><code>{col}</code></td>'
                       f'<td>{n:,}</td><td>{pct}%</td><td>{icon}</td></tr>')

    # Gráficos
    titulos = {
        'temperatura' : 'Temperatura 2m (°C)',
        'humidade'    : 'Humidade relativa (%)',
        'vento'       : 'Velocidade do vento (km/h)',
        'precipitacao': 'Precipitação (mm/dia)',
        'heatmap_temp': 'Heatmap temperatura — mês × hora do dia',
        'risco'       : 'Observações de risco alto (T + HR + Vento)',
        'correlacoes' : 'Correlação entre variáveis meteorológicas',
        'mapa_temp'   : 'Mapa espacial — temperatura média anual',
    }
    graficos_html = ''.join(
        img_html(graficos[k], t) for k, t in titulos.items() if k in graficos
    )

    risco_card = ''
    if 'obs_risco' in m:
        risco_card = (f'<div class="card" style="border-top-color:#e74c3c">'
                      f'<div class="valor" style="color:#c0392b">{m["obs_risco"]:,}</div>'
                      f'<div class="label">Obs. risco alto ({m["pct_risco"]}%)</div></div>')

    return f'''<!DOCTYPE html>
<html lang="pt">
<head>
<meta charset="UTF-8">
<title>ForestRisk — ERA5 EDA</title>
<style>
  body{{font-family:'Segoe UI',Arial,sans-serif;margin:0;background:#f5f5f5;color:#222}}
  .header{{background:linear-gradient(135deg,#1a5276,#2980b9);color:white;padding:36px 48px}}
  .header h1{{margin:0 0 6px;font-size:28px}}
  .header p{{margin:0;opacity:.85;font-size:14px}}
  .container{{max-width:1100px;margin:0 auto;padding:32px 24px}}
  h2{{font-size:20px;color:#1a5276;border-left:4px solid #2980b9;padding-left:12px;margin-top:40px}}
  h3{{font-size:15px;color:#444;margin-top:24px}}
  .cards{{display:flex;gap:16px;flex-wrap:wrap;margin:20px 0}}
  .card{{background:white;border-radius:10px;padding:20px 24px;flex:1;min-width:150px;
         box-shadow:0 1px 4px rgba(0,0,0,.08);border-top:3px solid #2980b9}}
  .card .valor{{font-size:28px;font-weight:700;color:#1a5276}}
  .card .label{{font-size:12px;color:#888;margin-top:4px}}
  .grafico{{background:white;border-radius:10px;padding:20px;
            box-shadow:0 1px 4px rgba(0,0,0,.08);margin:16px 0}}
  .grafico-titulo{{font-weight:600;color:#444;margin:0 0 12px;font-size:14px}}
  .grafico img{{width:100%;height:auto;border-radius:6px}}
  table.tabela{{border-collapse:collapse;width:100%;font-size:13px;margin:16px 0}}
  table.tabela th{{background:#1a5276;color:white;padding:10px 12px;text-align:left}}
  table.tabela td{{padding:8px 12px;border-bottom:1px solid #eee}}
  .box{{background:white;border-radius:10px;padding:24px;
        box-shadow:0 1px 4px rgba(0,0,0,.08);margin:16px 0}}
  .box ul{{margin:0;padding-left:20px;line-height:1.9}}
  code{{background:#f0f0f0;padding:2px 6px;border-radius:4px;font-size:12px}}
  .footer{{text-align:center;color:#aaa;font-size:12px;padding:32px 0}}
</style>
</head>
<body>
<div class="header">
  <h1>🌦️ ForestRisk — ERA5 Reanalysis EDA</h1>
  <p>Análise Exploratória · Copernicus Climate Data Store · ISEP Pós-Graduação 2024/2025</p>
  <p style="margin-top:8px;opacity:.7">Pessoa B — Data Scientist · Gerado em {now}</p>
</div>
<div class="container">

<h2>Resumo executivo</h2>
<div class="cards">
  <div class="card"><div class="valor">{m['n_registos']:,}</div><div class="label">Registos totais</div></div>
  <div class="card"><div class="valor">{m['n_pts_grid']}</div><div class="label">Pontos de grid PT</div></div>
  <div class="card"><div class="valor">{m.get('temp_media','—')}°C</div><div class="label">Temperatura média</div></div>
  <div class="card"><div class="valor">{m.get('temp_max','—')}°C</div><div class="label">Temperatura máxima</div></div>
  <div class="card"><div class="valor">{m.get('rh_media','—')}%</div><div class="label">Humidade média</div></div>
  <div class="card"><div class="valor">{m.get('vento_medio','—')} km/h</div><div class="label">Vento médio</div></div>
  {risco_card}
</div>

<div class="box"><ul>
  <li><strong>Período:</strong> {m['periodo_ini']} → {m['periodo_fim']}</li>
  <li><strong>Anos cobertos:</strong> {anos_str}</li>
  <li><strong>Resolução espacial:</strong> {m['n_pts_grid']} pontos de grid sobre Portugal (0.25° ≈ 25 km)</li>
  <li><strong>Temperatura:</strong> min {m.get('temp_min','—')}°C · média {m.get('temp_media','—')}°C · max {m.get('temp_max','—')}°C</li>
  <li><strong>Vento máximo registado:</strong> {m.get('vento_max','—')} km/h</li>
  <li><strong>Valores em falta:</strong> {m['nulos']:,} · Duplicados: {m['duplicados']:,}</li>
  <li><strong>Limiares de risco ForestRisk:</strong> T &gt; {TEMP_RISCO}°C + HR &lt; {HUM_RISCO}% + Vento &gt; {VENTO_RISCO} km/h</li>
</ul></div>

<h2>Variáveis ERA5 e derivadas</h2>
<table class="tabela">
  <tr><th>Variável ERA5</th><th>Nome no CSV</th><th>Unidade</th><th>Relevância ForestRisk</th></tr>
  <tr><td><code>t2m</code></td><td><code>temp_c</code></td><td>°C</td><td>Temperatura alta → risco de ignição</td></tr>
  <tr><td><code>d2m + t2m</code></td><td><code>rh</code></td><td>%</td><td>Humidade baixa → combustível seco</td></tr>
  <tr><td><code>u10 + v10</code></td><td><code>wind_speed_kmh</code></td><td>km/h</td><td>Vento forte → propagação rápida</td></tr>
  <tr><td><code>mtpr</code></td><td><code>precip_mm_dia</code></td><td>mm/dia</td><td>Precipitação baixa → seca prolongada</td></tr>
  <tr><td><code>mer</code></td><td><code>evap_mm_dia</code></td><td>mm/dia</td><td>Evaporação elevada → secura do solo</td></tr>
  <tr><td><code>cvh</code></td><td><code>cvh</code></td><td>0–1</td><td>Cobertura vegetal → carga de combustível</td></tr>
  <tr><td><code>tvl</code></td><td><code>tvl</code></td><td>código</td><td>Tipo de vegetação baixa</td></tr>
</table>

<h2>Qualidade dos dados</h2>
<h3>Valores em falta por coluna</h3>
<table class="tabela">
  <tr><th>Coluna</th><th>Nulls</th><th>%</th><th>Estado</th></tr>
  {nulos_rows or '<tr><td colspan="4" style="text-align:center;color:#27ae60">✅ Sem valores em falta</td></tr>'}
</table>
<div class="box"><ul>
  <li><strong>Duplicados:</strong> {m['duplicados']:,} {"⚠️ serão removidos" if m['duplicados'] > 0 else "✅ sem duplicados"}</li>
</ul></div>

<h2>Estatísticas descritivas</h2>
{stats_html}

<h2>Visualizações</h2>
{graficos_html}

<h2>Próximo passo: juntar com FIRMS</h2>
<div class="box"><ul>
  <li>Join espacial-temporal entre ERA5 e FIRMS por coordenada (grid 5×5 km) e data</li>
  <li>Calcular índice de risco composto 0–100 por célula de grid</li>
  <li>Usar variáveis ERA5 (<code>temp_c</code>, <code>rh</code>, <code>wind_speed_kmh</code>) como features no Random Forest + XGBoost</li>
</ul></div>

</div>
<div class="footer">ForestRisk · ISEP 2024/2025 · ERA5 Copernicus · {now}</div>
</body>
</html>'''

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print('\n🌦️  ForestRisk — ERA5 EDA Automático')
    print('=' * 45)

    # Verificar xarray
    try:
        import xarray as xr
    except ImportError:
        print('❌ xarray não instalado.')
        print('   Corre: pip install xarray netcdf4 h5netcdf')
        sys.exit(1)

    # 1. Encontrar ficheiros
    print('\n📂 A procurar ficheiros ERA5...')
    nc_files = encontrar_nc()

    # 2. Carregar e filtrar Portugal
    print('\n📥 A carregar ERA5 (Portugal)...')
    ds_pt = carregar_era5(nc_files)

    # 3. Converter para DataFrame
    print('\n🔄 A converter para DataFrame...')
    df = ds_pt.to_dataframe().reset_index()
    if 'valid_time' in df.columns and 'time' not in df.columns:
        df = df.rename(columns={'valid_time': 'time'})
    df = df.dropna(subset=['latitude','longitude'])
    print(f'  📊 {len(df):,} registos · {df.shape[1]} colunas')

    # 4. Calcular derivadas
    print('\n⚙️  A calcular variáveis derivadas...')
    df = calcular_derivadas(df)
    derivadas = [c for c in ['temp_c','rh','wind_speed_kmh','precip_mm_dia','evap_mm_dia'] if c in df.columns]
    print(f'  ✅ Derivadas calculadas: {derivadas}')

    # 5. Métricas
    print('\n📈 A calcular métricas...')
    m = calcular_metricas(df)

    # 6. Estatísticas descritivas
    cols_s   = [c for c in ['temp_c','rh','wind_speed_kmh','precip_mm_dia','evap_mm_dia'] if c in df.columns]
    stats_html = df[cols_s].describe().round(2).to_html(classes='tabela', border=0) if cols_s else ''

    # 7. Gráficos
    print('\n📊 A gerar gráficos...')
    graficos = analisar(df)
    print(f'  ✅ {len(graficos)} gráficos gerados')

    # 8. Relatório HTML
    print('\n📝 A gerar relatório HTML...')
    html = gerar_html(m, graficos, stats_html)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)

    # 9. Guardar outputs
    COLS = [c for c in ['time','latitude','longitude','ano','mes','dia','hora',
                         'temp_c','dewpoint_c','rh','wind_speed_kmh',
                         'precip_mm_dia','evap_mm_dia','cvh','tvl','risco_alto']
            if c in df.columns]
    df_out = df[COLS].copy()

    PASTA_CSV.mkdir(exist_ok=True)
    PASTA_PARQUET.mkdir(exist_ok=True)

    print('\n💾 A guardar ficheiros por ano...')
    anos_guardados = []
    for ano, df_ano in df_out.groupby('ano'):
        f_csv     = PASTA_CSV     / f'era5_portugal_{ano}.csv'
        f_parquet = PASTA_PARQUET / f'era5_portugal_{ano}.parquet'
        df_ano.to_csv(f_csv, index=False)
        try:
            df_ano.to_parquet(f_parquet, index=False)
            print(f'  ✅ {ano}: {len(df_ano):,} registos → {f_csv.name} + {f_parquet.name}')
        except Exception:
            print(f'  ✅ {ano}: {len(df_ano):,} registos → {f_csv.name}')
        anos_guardados.append(str(ano))

    df_out.to_csv(PASTA_CSV / 'era5_portugal_todos.csv', index=False)
    try:
        df_out.to_parquet(PASTA_PARQUET / 'era5_portugal_todos.parquet', index=False)
    except Exception:
        pass

    print(f'\n✅ Relatório      : {OUTPUT_HTML}')
    print(f'✅ CSV por ano    : ERA5_CSV/era5_portugal_[{", ".join(anos_guardados)}].csv')
    print(f'✅ Parquet por ano: ERA5_Parquet/era5_portugal_[{", ".join(anos_guardados)}].parquet')
    print(f'\n   Abre {OUTPUT_HTML} no browser para ver o relatório completo.')
    print('=' * 45)

if __name__ == '__main__':
    main()
