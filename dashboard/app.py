"""
dashboard/app.py
🏔️ Data Lakehouse ENEM — Dashboard
Consome o Gold layer do DuckDB (construído pelo dbt sobre o Delta Lake).
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os, sys

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DW_PATH = os.path.join(ROOT, "data", "warehouse", "lakehouse.duckdb")

st.set_page_config(page_title="Lakehouse ENEM", page_icon="🏔️", layout="wide")

# ─── CSS ─────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
* { font-family: 'Inter', sans-serif; }

[data-testid="stAppViewContainer"], .block-container {
    background-color: #0f1117 !important; color: #e2e8f0 !important;
}
[data-testid="stSidebar"], [data-testid="stSidebar"] > div {
    background-color: #090c10 !important;
}
[data-testid="stSidebar"] * { color: #94a3b8 !important; }

.hero {
    background: linear-gradient(135deg, #1e3a5f 0%, #0f2a47 60%, #091929 100%);
    border-radius: 12px; padding: 2.5rem 3rem; margin-bottom: 2rem;
    border: 1px solid #1e40af33;
}
.hero h1 { font-size: 2.2rem; color: #93c5fd; margin: 0; font-weight: 700; }
.hero h1 span { color: #60a5fa; }
.hero p  { color: #64748b; font-size: 0.9rem; margin: 0.4rem 0 0; }

.badge {
    display: inline-block; padding: 0.2rem 0.7rem; border-radius: 20px;
    font-size: 0.7rem; font-weight: 600; letter-spacing: 0.08em;
    text-transform: uppercase; margin: 0.2rem;
}
.badge-delta   { background: #1e3a5f; color: #93c5fd; border: 1px solid #3b82f655; }
.badge-spark   { background: #3b1f0a; color: #fb923c; border: 1px solid #f9731655; }
.badge-dbt     { background: #1a2e1a; color: #86efac; border: 1px solid #22c55e55; }
.badge-duckdb  { background: #2d1b4e; color: #c4b5fd; border: 1px solid #a78bfa55; }

.kpi {
    background: #141b2d; border: 1px solid #1e293b;
    border-top: 3px solid #3b82f6;
    border-radius: 8px; padding: 1.2rem 1.4rem;
}
.kpi-val { font-size: 1.9rem; font-weight: 700; color: #60a5fa; margin: 0.2rem 0; }
.kpi-lbl { font-size: 0.7rem; color: #475569; text-transform: uppercase;
           letter-spacing: 0.1em; font-weight: 600; }
.kpi-sub { font-size: 0.78rem; color: #334155; margin-top: 0.2rem; }

.sec { font-size: 1rem; font-weight: 600; color: #93c5fd;
       border-bottom: 1px solid #1e293b; padding-bottom: 0.4rem;
       margin: 1.5rem 0 0.8rem; }

.tt-card {
    background: #0d1829; border: 1px solid #1e3a5f;
    border-left: 4px solid #3b82f6;
    border-radius: 8px; padding: 1.2rem 1.5rem; margin: 0.4rem 0;
}
.tt-card .tt-title { font-weight: 600; color: #93c5fd; font-size: 0.9rem; }
.tt-card .tt-desc  { color: #475569; font-size: 0.82rem; margin-top: 0.3rem; line-height: 1.6; }
.tt-card code { background: #1e293b; color: #86efac; padding: 0.15rem 0.4rem;
                border-radius: 4px; font-size: 0.8rem; }
</style>
""", unsafe_allow_html=True)

PL_DARK = dict(
    paper_bgcolor="#0f1117", plot_bgcolor="#141b2d",
    font=dict(family="Inter", color="#94a3b8", size=11),
    xaxis=dict(gridcolor="#1e293b", linecolor="#1e293b", tickfont=dict(color="#64748b")),
    yaxis=dict(gridcolor="#1e293b", linecolor="#1e293b", tickfont=dict(color="#64748b")),
    margin=dict(t=30, b=30, l=30, r=16),
)
BLUE   = "#3b82f6"
CYAN   = "#06b6d4"
GREEN  = "#22c55e"
ORANGE = "#f97316"
PURPLE = "#a78bfa"
COLORS = [BLUE, CYAN, GREEN, ORANGE, PURPLE, "#ec4899", "#eab308"]


# ─── Helpers ─────────────────────────────────────────────────────
@st.cache_data(ttl=120)
def query(sql: str) -> pd.DataFrame:
    for ro in (True, False):
        try:
            conn = duckdb.connect(DW_PATH, read_only=ro)
            # Auto-detecta schema gold (pode ser 'gold' ou 'main_gold')
            gold_schema = "gold"
            for s in ("gold", "main_gold"):
                try:
                    conn.execute(f"SELECT 1 FROM {s}.gld_desempenho_uf LIMIT 1")
                    gold_schema = s
                    break
                except Exception:
                    continue
            resolved = sql.replace("{schema}", gold_schema).replace("main_gold.", f"{gold_schema}.")
            df = conn.execute(resolved).df()
            conn.close()
            return df
        except Exception:
            continue
    return pd.DataFrame()

def dw_ok() -> bool:
    if not os.path.exists(DW_PATH):
        return False
    for readonly in (True, False):
        for schema in ("gold", "main_gold"):
            try:
                conn = duckdb.connect(DW_PATH, read_only=readonly)
                conn.execute(f"SELECT 1 FROM {schema}.gld_desempenho_uf LIMIT 1")
                conn.close()
                return True
            except Exception:
                continue
    return False

def _gold_schema(conn) -> str:
    """Detecta se o schema gold foi criado como 'gold' ou 'main_gold'."""
    for s in ("gold", "main_gold"):
        try:
            conn.execute(f"SELECT 1 FROM {s}.gld_desempenho_uf LIMIT 1")
            return s
        except Exception:
            continue
    return "gold"

def Q(table: str, extra: str = "") -> pd.DataFrame:
    for ro in (True, False):
        try:
            conn = duckdb.connect(DW_PATH, read_only=ro)
            schema = _gold_schema(conn)
            df = conn.execute(f"SELECT * FROM {schema}.{table} {extra}").df()
            conn.close()
            return df
        except Exception:
            continue
    return pd.DataFrame()


# ─── Sidebar ─────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding:1rem 0 1.5rem; border-bottom:1px solid #1e293b;'>
        <div style='font-size:1.2rem; font-weight:700; color:#60a5fa;'>🏔️ Lakehouse ENEM</div>
        <div style='font-size:0.72rem; color:#334155; margin-top:0.3rem; line-height:1.7;'>
            Delta Lake · PySpark · dbt · DuckDB
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    page = st.radio("Navegação", [
        "🏠 Visão Geral",
        "🗺️  Por UF",
        "💰 Renda & Escola",
        "📈 Evolução Anual",
        "⏱️  Time Travel",
        "⚙️  Arquitetura",
    ], label_visibility="collapsed")

    if st.button("🔄 Recarregar"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("""
    <div style='margin-top:2rem; font-size:0.7rem; color:#1e293b; line-height:2.2;'>
        <span style='color:#fb923c;'>●</span> Bronze — Delta Lake raw<br>
        <span style='color:#64748b;'>●</span> Silver — Delta + schema evolution<br>
        <span style='color:#3b82f6;'>●</span> Gold — dbt + DuckDB<br>
        <span style='color:#1e293b;'>──────────────</span><br>
        PySpark 3.5 · Delta 3.2<br>
        dbt-duckdb 1.8<br>
        Streamlit · Plotly
    </div>
    """, unsafe_allow_html=True)


# ─── Bootstrap ───────────────────────────────────────────────────
if not dw_ok():
    st.markdown("""
    <div style='background:#0d1829; border:1px solid #1e3a5f; border-left:4px solid #3b82f6;
         border-radius:8px; padding:2rem 2.5rem; margin-top:2rem;'>
        <div style='font-size:1.3rem; font-weight:700; color:#60a5fa; margin-bottom:0.5rem;'>
            🏔️ Warehouse não encontrado
        </div>
        <p style='color:#475569; margin:0;'>
            Execute o pipeline primeiro, depois clique em 🔄 Recarregar.
        </p>
    </div>
    """, unsafe_allow_html=True)
    st.code("python run.py --only-pipeline", language="bash")
    st.stop()


# ════════════════════════════════════════════════════════════════
# VISÃO GERAL
# ════════════════════════════════════════════════════════════════
if page == "🏠 Visão Geral":
    st.markdown("""
    <div class="hero">
        <h1>🏔️ Data <span>Lakehouse</span> ENEM</h1>
        <p>Delta Lake · PySpark · dbt · DuckDB · Microdados ENEM 2020–2022</p>
        <div style='margin-top:0.8rem;'>
            <span class="badge badge-delta">Delta Lake</span>
            <span class="badge badge-spark">PySpark 3.5</span>
            <span class="badge badge-dbt">dbt</span>
            <span class="badge badge-duckdb">DuckDB</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    df = Q("gld_desempenho_uf", "WHERE ano = 2022")
    kpis = [
        ("#3b82f6", f"{df['total_candidatos'].sum():,.0f}", "Candidatos 2022", "registros gold"),
        ("#06b6d4", f"{df['media_geral'].mean():.1f}",     "Média Geral",      "Brasil 2022"),
        ("#22c55e", f"{df['media_matematica'].mean():.1f}","Média Matemática",  "Brasil 2022"),
        ("#a78bfa", f"{df['pct_escola_publica'].mean():.0f}%", "Escola Pública", "dos candidatos"),
        ("#f97316", f"{df['uf'].nunique()}",                "UFs analisadas",   "gold layer"),
    ]
    cols = st.columns(5)
    for col, (accent, val, lbl, sub) in zip(cols, kpis):
        with col:
            st.markdown(f"""
            <div class="kpi" style="border-top-color:{accent};">
                <div class="kpi-lbl">{lbl}</div>
                <div class="kpi-val" style="color:{accent};">{val}</div>
                <div class="kpi-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown('<div class="sec">Média Geral por Região — 2022</div>', unsafe_allow_html=True)
        reg = query("""
            SELECT regiao, ROUND(AVG(media_geral),2) AS media, SUM(total_candidatos) AS n
            FROM {schema}.gld_desempenho_uf WHERE ano = 2022 GROUP BY regiao ORDER BY media DESC
        """)
        fig = px.bar(reg, x="regiao", y="media", color="regiao",
                     color_discrete_sequence=COLORS, text="media")
        fig.update_traces(texttemplate="%{text:.1f}", textposition="outside", textfont_size=10)
        fig.update_layout(**PL_DARK, height=320, showlegend=False)
        st.plotly_chart(fig, width="stretch")

    with col_b:
        st.markdown('<div class="sec">Distribuição de Desempenho — 2022</div>', unsafe_allow_html=True)
        dist = query("""
            SELECT faixa_desempenho, COUNT(*) AS n
            FROM {schema}.gld_desempenho_uf
            -- nota: faixa não está no gold, vamos usar percentil
            -- fallback: média por faixa via silver diretamente
            WHERE 1=0
        """)
        # Usa os dados que temos
        fig2 = px.scatter(df, x="pct_escola_publica", y="media_geral",
                          size="total_candidatos", color="regiao", text="uf",
                          color_discrete_sequence=COLORS, size_max=30, opacity=0.8)
        fig2.update_traces(textposition="top center", textfont_size=8)
        fig2.update_layout(**PL_DARK, height=320,
                           xaxis_title="% Escola Pública",
                           yaxis_title="Média Geral",
                           legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig2, width="stretch")


# ════════════════════════════════════════════════════════════════
# POR UF
# ════════════════════════════════════════════════════════════════
elif page == "🗺️  Por UF":
    st.markdown("""
    <div class="hero">
        <h1>🗺️ Desempenho por <span>UF</span></h1>
        <p>gold.gld_desempenho_uf · Médias e rankings por estado</p>
    </div>""", unsafe_allow_html=True)

    df_all = Q("gld_desempenho_uf")
    anos   = sorted(df_all["ano"].unique())
    ano    = st.select_slider("Ano", options=anos, value=max(anos))
    df     = df_all[df_all["ano"] == ano]

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="sec">Ranking — Média Geral</div>', unsafe_allow_html=True)
        rank = df.sort_values("media_geral", ascending=True)
        fig  = go.Figure(go.Bar(
            x=rank["media_geral"], y=rank["uf"], orientation="h",
            marker=dict(color=rank["media_geral"],
                        colorscale=[[0, "#1e3a5f"], [0.5, BLUE], [1, CYAN]],
                        showscale=False),
            text=rank["media_geral"].round(1), textposition="outside", textfont_size=9,
        ))
        fig.update_layout(**PL_DARK, height=500, xaxis_title="Média Geral")
        st.plotly_chart(fig, width="stretch")

    with col_b:
        st.markdown('<div class="sec">Escola Pública vs. Nota — UFs</div>', unsafe_allow_html=True)
        fig2 = px.scatter(df, x="pct_escola_publica", y="media_geral",
                          size="total_candidatos", color="regiao",
                          text="uf", color_discrete_sequence=COLORS,
                          size_max=35, opacity=0.85)
        fig2.update_traces(textposition="top center", textfont_size=8)
        fig2.update_layout(**PL_DARK, height=500, legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig2, width="stretch")

    st.markdown('<div class="sec">Tabela Completa</div>', unsafe_allow_html=True)
    st.dataframe(df.sort_values("media_geral", ascending=False), width="stretch", hide_index=True)


# ════════════════════════════════════════════════════════════════
# RENDA & ESCOLA
# ════════════════════════════════════════════════════════════════
elif page == "💰 Renda & Escola":
    st.markdown("""
    <div class="hero">
        <h1>💰 Renda & <span>Escola</span></h1>
        <p>gold.gld_desempenho_renda · Desigualdade educacional nos dados do ENEM</p>
    </div>""", unsafe_allow_html=True)

    df = Q("gld_desempenho_renda", "WHERE ano = 2022")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="sec">Média Geral por Grupo de Renda</div>', unsafe_allow_html=True)
        grp = df.groupby("grupo_renda")["media_geral"].mean().reset_index().sort_values("media_geral")
        fig = px.bar(grp, x="grupo_renda", y="media_geral", color="grupo_renda",
                     color_discrete_sequence=COLORS, text="media_geral")
        fig.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        fig.update_layout(**PL_DARK, height=320, showlegend=False)
        st.plotly_chart(fig, width="stretch")

    with col_b:
        st.markdown('<div class="sec">Pública vs Privada — Média por Renda</div>', unsafe_allow_html=True)
        pivot = df.pivot_table(values="media_geral", index="grupo_renda",
                               columns="tp_escola", aggfunc="mean").reset_index()
        fig2 = go.Figure()
        for i, col in enumerate([c for c in pivot.columns if c != "grupo_renda"]):
            fig2.add_trace(go.Bar(name=col, x=pivot["grupo_renda"],
                                  y=pivot[col].round(1), marker_color=COLORS[i]))
        fig2.update_layout(**PL_DARK, height=320, barmode="group",
                           legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig2, width="stretch")

    st.markdown('<div class="sec">% Bom ou Excelente por Renda e Escola</div>', unsafe_allow_html=True)
    fig3 = px.density_heatmap(df, x="grupo_renda", y="tp_escola",
                               z="pct_bom_ou_excelente", histfunc="avg",
                               color_continuous_scale=[[0,"#0f1117"],[0.5,BLUE],[1,CYAN]])
    fig3.update_layout(**{**PL_DARK, "margin": dict(t=30, b=30, l=120, r=16)}, height=300)
    st.plotly_chart(fig3, width="stretch")


# ════════════════════════════════════════════════════════════════
# EVOLUÇÃO ANUAL
# ════════════════════════════════════════════════════════════════
elif page == "📈 Evolução Anual":
    st.markdown("""
    <div class="hero">
        <h1>📈 Evolução <span>Anual</span></h1>
        <p>gold.gld_evolucao_anual · Variação YoY por região · dbt window functions</p>
    </div>""", unsafe_allow_html=True)

    df = Q("gld_evolucao_anual")
    regioes  = sorted(df["regiao"].dropna().unique())
    sel_regs = st.multiselect("Regiões", regioes, default=regioes)
    df_f     = df[df["regiao"].isin(sel_regs)] if sel_regs else df

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="sec">Média Geral por Ano e Região</div>', unsafe_allow_html=True)
        fig = px.line(df_f, x="ano", y="media_geral", color="regiao",
                      markers=True, color_discrete_sequence=COLORS)
        fig.update_traces(line_width=2.5, marker_size=7)
        fig.update_layout(**PL_DARK, height=320, legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig, width="stretch")

    with col_b:
        st.markdown('<div class="sec">Variação YoY (%)</div>', unsafe_allow_html=True)
        df_yoy = df_f.dropna(subset=["variacao_yoy_pct"])
        fig2   = px.bar(df_yoy, x="ano", y="variacao_yoy_pct", color="regiao",
                        barmode="group", color_discrete_sequence=COLORS)
        fig2.add_hline(y=0, line_dash="dot", line_color="#334155")
        fig2.update_layout(**PL_DARK, height=320, legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig2, width="stretch")


# ════════════════════════════════════════════════════════════════
# TIME TRAVEL
# ════════════════════════════════════════════════════════════════
elif page == "⏱️  Time Travel":
    st.markdown("""
    <div class="hero">
        <h1>⏱️ Delta Lake <span>Time Travel</span></h1>
        <p>Consulta qualquer versão histórica dos dados — sem backups manuais</p>
    </div>""", unsafe_allow_html=True)

    features = [
        ("VERSION AS OF",
         "Consulta o estado exato da tabela em uma versão anterior.",
         'df = spark.read.format("delta").option("versionAsOf", 0).load(PATH)'),
        ("TIMESTAMP AS OF",
         "Consulta como os dados estavam em qualquer data/hora passada.",
         'df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(PATH)'),
        ("HISTORY",
         "Auditoria completa de todas as operações: quem escreveu, quando, qual operação.",
         'DeltaTable.forPath(spark, PATH).history().show()'),
        ("ROLLBACK",
         "Restaura a tabela para qualquer versão anterior com uma linha.",
         'RESTORE TABLE delta.`path` TO VERSION AS OF 2'),
        ("Schema Evolution",
         "Adiciona colunas novas sem recriar a tabela. Versões antigas ficam acessíveis com o schema original.",
         '.option("mergeSchema", "true")'),
        ("ACID Transactions",
         "MERGE garante consistência: se o job falhar no meio, a tabela não fica em estado parcial.",
         'deltaTable.merge(source, condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()'),
    ]

    for title, desc, code in features:
        st.markdown(f"""
        <div class="tt-card">
            <div class="tt-title">{title}</div>
            <div class="tt-desc">{desc}<br><br><code>{code}</code></div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sec">Como rodar o demo de Time Travel</div>', unsafe_allow_html=True)
    st.code("python spark_jobs/time_travel.py", language="bash")
    st.info("O script executa todas as 6 demos acima no terminal e imprime os resultados. "
            "A pasta `data/delta/silver/enem/_delta_log/` guarda o histórico completo de versões.")


# ════════════════════════════════════════════════════════════════
# ARQUITETURA
# ════════════════════════════════════════════════════════════════
elif page == "⚙️  Arquitetura":
    st.markdown("""
    <div class="hero">
        <h1>⚙️ <span>Arquitetura</span> do Lakehouse</h1>
        <p>Delta Lake · PySpark · dbt · DuckDB · Streamlit</p>
    </div>""", unsafe_allow_html=True)

    steps = [
        ("🟠", "INGESTÃO",        "ingestion/download.py — gera microdados ENEM simulados (estrutura real do INEP)<br>Saída: data/raw/enem_{ano}.csv"),
        ("🟠", "BRONZE",          "spark_jobs/bronze.py — PySpark lê os CSVs com schema explícito e escreve em Delta Lake<br>Particionado por ano · colunas de auditoria _bronze_loaded_at, _source_file"),
        ("⚪", "SILVER",          "spark_jobs/silver.py — limpeza, médias, faixas de desempenho<br>ACID MERGE: upsert atômico · Schema Evolution: percentil_nacional adicionado sem recriar"),
        ("🔵", "BRIDGE",          "spark_jobs/delta_to_duckdb.py — exporta Silver para Parquet e registra como view no DuckDB<br>Ponte entre o mundo Spark e o mundo dbt"),
        ("🔵", "GOLD (dbt)",      "dbt lê silver.enem via DuckDB e materializa 3 modelos gold<br>gld_desempenho_uf · gld_desempenho_renda · gld_evolucao_anual"),
        ("🟢", "TIME TRAVEL",     "spark_jobs/time_travel.py — demonstra VERSION AS OF, TIMESTAMP AS OF, HISTORY, ROLLBACK<br>_delta_log/ guarda todas as versões automaticamente"),
        ("🟣", "DASHBOARD",       "dashboard/app.py — Streamlit consome o gold layer via DuckDB read-only<br>5 páginas analíticas + página dedicada ao Time Travel"),
    ]

    for icon, title, desc in steps:
        st.markdown(f"""
        <div class="tt-card">
            <div class="tt-title">{icon} {title}</div>
            <div class="tt-desc">{desc}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sec">Stack</div>', unsafe_allow_html=True)
    st.dataframe(pd.DataFrame([
        ["Armazenamento",  "Delta Lake 3.2",     "Formato ACID com time travel e schema evolution"],
        ["Processamento",  "PySpark 3.5",         "Transformações distribuídas, merge, particionamento"],
        ["Transformação",  "dbt-duckdb 1.8",      "Gold layer com SQL + testes de qualidade"],
        ["Query Engine",   "DuckDB 0.10",          "Lê Parquet nativamente, serve o dashboard"],
        ["Dashboard",      "Streamlit + Plotly",   "Consome gold layer via DuckDB read-only"],
        ["CI/CD",          "GitHub Actions",       "dbt parse → run → test a cada push"],
    ], columns=["Camada", "Tecnologia", "Papel"]), width="stretch", hide_index=True)
