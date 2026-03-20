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


# ─── Schema detection ────────────────────────────────────────────
def _detect_gold_schema(conn) -> str:
    """
    Detecta em qual schema o dbt criou as tabelas gold.
    dbt-duckdb pode criar em 'main', 'main_gold', 'gold' dependendo da config.
    """
    candidates = ["main", "main_gold", "gold"]
    for s in candidates:
        try:
            conn.execute(f"SELECT 1 FROM {s}.gld_desempenho_uf LIMIT 1")
            return s
        except Exception:
            continue
    return "main"


@st.cache_data(ttl=120)
def _get_gold_schema() -> str:
    for ro in (True, False):
        try:
            conn = duckdb.connect(DW_PATH, read_only=ro)
            schema = _detect_gold_schema(conn)
            conn.close()
            return schema
        except Exception:
            continue
    return "main"


def query(sql: str) -> pd.DataFrame:
    schema = _get_gold_schema()
    resolved = sql.replace("{GOLD}", schema)
    for ro in (True, False):
        try:
            conn = duckdb.connect(DW_PATH, read_only=ro)
            df   = conn.execute(resolved).df()
            conn.close()
            return df
        except Exception:
            continue
    return pd.DataFrame()


def dw_ok() -> bool:
    if not os.path.exists(DW_PATH):
        return False
    for ro in (True, False):
        try:
            conn = duckdb.connect(DW_PATH, read_only=ro)
            _detect_gold_schema(conn)  # raises if not found
            # test actual query
            schema = _detect_gold_schema(conn)
            conn.execute(f"SELECT 1 FROM {schema}.gld_desempenho_uf LIMIT 1")
            conn.close()
            return True
        except Exception:
            continue
    return False


def Q(table: str, extra: str = "") -> pd.DataFrame:
    return query(f"SELECT * FROM {{GOLD}}.{table} {extra}")


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
        ("#3b82f6", f"{df['total_candidatos'].sum():,.0f}", "Candidatos 2022",   "registros gold"),
        ("#06b6d4", f"{df['media_geral'].mean():.1f}",      "Média Geral",        "Brasil 2022"),
        ("#22c55e", f"{df['media_matematica'].mean():.1f}", "Média Matemática",   "Brasil 2022"),
        ("#a78bfa", f"{df['pct_escola_publica'].mean():.0f}%", "Escola Pública", "dos candidatos"),
        ("#f97316", f"{df['uf'].nunique()}",                 "UFs analisadas",    "gold layer"),
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
            FROM {GOLD}.gld_desempenho_uf WHERE ano = 2022 GROUP BY regiao ORDER BY media DESC
        """)
        fig = px.bar(reg, x="regiao", y="media", color="regiao",
                     color_discrete_sequence=COLORS, text="media")
        fig.update_traces(texttemplate="%{text:.1f}", textposition="outside", textfont_size=10)
        fig.update_layout(**PL_DARK, height=320, showlegend=False)
        st.plotly_chart(fig, width="stretch")

    with col_b:
        st.markdown('<div class="sec">Escola Pública × Média Geral — UFs 2022</div>', unsafe_allow_html=True)
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
        <p>gld_desempenho_uf · Médias e rankings por estado</p>
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
        st.markdown('<div class="sec">Escola Pública vs. Nota</div>', unsafe_allow_html=True)
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
        <p>gld_desempenho_renda · Desigualdade educacional nos dados do ENEM</p>
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
        st.markdown('<div class="sec">Pública vs Privada — por Renda</div>', unsafe_allow_html=True)
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
        <p>gld_evolucao_anual · Variação YoY por região · dbt window functions</p>
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
        <p>Demo interativo ao vivo — consulta versões históricas reais do Delta Lake</p>
    </div>""", unsafe_allow_html=True)

    SILVER_DELTA = os.path.join(ROOT, "data", "delta", "silver", "enem")

    def delta_available() -> bool:
        log_dir = os.path.join(SILVER_DELTA, "_delta_log")
        return os.path.isdir(log_dir) and len(os.listdir(log_dir)) > 0

    # ── Carrega histórico do _delta_log sem Spark ─────────────────
    # O _delta_log é JSON — lemos diretamente, sem precisar do PySpark
    @st.cache_data(ttl=60)
    def load_delta_history() -> pd.DataFrame:
        import json, glob
        log_dir = os.path.join(SILVER_DELTA, "_delta_log")
        rows = []
        for f in sorted(glob.glob(os.path.join(log_dir, "*.json"))):
            with open(f, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if "commitInfo" in obj:
                            ci = obj["commitInfo"]
                            rows.append({
                                "versão":    ci.get("version", "?"),
                                "timestamp": pd.to_datetime(ci.get("timestamp", 0), unit="ms"),
                                "operação":  ci.get("operation", "?"),
                                "parâmetros": str(ci.get("operationParameters", "")),
                            })
                    except Exception:
                        continue
        return pd.DataFrame(rows).sort_values("versão").reset_index(drop=True)

    @st.cache_data(ttl=60)
    def load_delta_schema_per_version() -> dict:
        """Lê o schema de cada versão a partir dos metadados do _delta_log."""
        import json, glob
        log_dir = os.path.join(SILVER_DELTA, "_delta_log")
        schemas = {}
        current_version = None
        for f in sorted(glob.glob(os.path.join(log_dir, "*.json"))):
            with open(f, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if "commitInfo" in obj:
                            current_version = obj["commitInfo"].get("version")
                        if "metaData" in obj and current_version is not None:
                            schema_str = obj["metaData"].get("schemaString", "{}")
                            schema_obj = json.loads(schema_str)
                            cols = [f["name"] for f in schema_obj.get("fields", [])]
                            schemas[current_version] = cols
                    except Exception:
                        continue
        return schemas

    @st.cache_data(ttl=60)
    def load_parquet_stats() -> pd.DataFrame:
        """Conta registros por partição nos arquivos Parquet do Silver."""
        import glob
        rows = []
        for f in glob.glob(os.path.join(SILVER_DELTA, "**", "*.parquet"), recursive=True):
            parts = f.replace("\\", "/").split("/")
            ano = next((p.split("=")[1] for p in parts if p.startswith("ano=")), "?")
            regiao = next((p.split("=")[1] for p in parts if p.startswith("regiao=")), "?")
            size_kb = os.path.getsize(f) / 1024
            rows.append({"ano": ano, "regiao": regiao, "arquivo": os.path.basename(f), "tamanho_kb": round(size_kb, 1)})
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    # ─────────────────────────────────────────────────────────────
    if not delta_available():
        st.warning("Delta Lake Silver não encontrado. Execute o pipeline primeiro.")
        st.code("python run.py --only-pipeline", language="bash")
    else:
        history = load_delta_history()
        schemas = load_delta_schema_per_version()

        # ── BLOCO 1: Linha do tempo das versões ──────────────────
        st.markdown('<div class="sec">📋 Histórico de Versões — _delta_log</div>', unsafe_allow_html=True)
        st.caption("Cada linha é uma transação registrada automaticamente pelo Delta Lake no arquivo `_delta_log/*.json`")

        if not history.empty:
            # Linha do tempo visual
            fig_hist = px.scatter(
                history, x="timestamp", y=[0]*len(history),
                text="versão", color="operação",
                color_discrete_sequence=COLORS,
                size=[20]*len(history), size_max=20,
            )
            fig_hist.update_traces(textposition="top center", textfont_size=11)
            pl_timeline = {k: v for k, v in PL_DARK.items() if k not in ("yaxis", "margin")}
            fig_hist.update_layout(
                **pl_timeline,
                margin=dict(t=40, b=10, l=10, r=10),
                height=160, showlegend=True,
                yaxis=dict(visible=False, range=[-1, 1]),
                xaxis_title="",
                legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=1.3),
            )
            st.plotly_chart(fig_hist, width="stretch")

            # Tabela detalhada
            st.dataframe(history, width="stretch", hide_index=True)
        else:
            st.info("Nenhuma entrada de commitInfo encontrada no _delta_log.")

        # ── BLOCO 2: Schema Evolution ao vivo ────────────────────
        st.markdown('<div class="sec">🧬 Schema Evolution — Colunas por Versão</div>', unsafe_allow_html=True)
        st.caption("Demonstra como o Delta Lake permite adicionar colunas sem recriar a tabela — versões antigas continuam acessíveis com o schema original.")

        if schemas:
            all_cols = sorted({c for cols in schemas.values() for c in cols})
            matrix = []
            for v in sorted(schemas.keys()):
                row = {"Versão": f"v{v}"}
                for c in all_cols:
                    row[c] = "✅" if c in schemas[v] else "—"
                matrix.append(row)
            df_schema = pd.DataFrame(matrix).set_index("Versão")

            # Destaca colunas novas (aparecem em versões posteriores)
            st.dataframe(df_schema, width="stretch")

            # Explica a coluna adicionada
            new_cols = []
            versions = sorted(schemas.keys())
            if len(versions) >= 2:
                v0_cols = set(schemas[versions[0]])
                vlast_cols = set(schemas[versions[-1]])
                new_cols = list(vlast_cols - v0_cols)

            if new_cols:
                st.markdown(f"""
                <div class="tt-card">
                    <div class="tt-title">🔬 Coluna adicionada via Schema Evolution</div>
                    <div class="tt-desc">
                        A coluna <code>{'</code>, <code>'.join(new_cols)}</code> não existia na v0.
                        Foi adicionada com <code>.option("mergeSchema", "true")</code> após o MERGE,
                        sem recriar a tabela ou perder o histórico anterior.
                        A versão 0 continua acessível sem essa coluna.
                    </div>
                </div>""", unsafe_allow_html=True)

        # ── BLOCO 3: VERSION AS OF — comparação ao vivo ──────────
        st.markdown('<div class="sec">⏪ VERSION AS OF — Comparação entre Versões</div>', unsafe_allow_html=True)
        st.caption("Selecione duas versões para comparar o estado dos dados em cada momento.")

        versions_available = sorted(schemas.keys())
        if len(versions_available) >= 2:
            col_sel1, col_sel2 = st.columns(2)
            with col_sel1:
                v_a = st.selectbox("Versão A", versions_available,
                                   index=0, format_func=lambda x: f"v{x}")
            with col_sel2:
                v_b = st.selectbox("Versão B", versions_available,
                                   index=len(versions_available)-1,
                                   format_func=lambda x: f"v{x}")

            cols_a = schemas.get(v_a, [])
            cols_b = schemas.get(v_b, [])
            added   = sorted(set(cols_b) - set(cols_a))
            removed = sorted(set(cols_a) - set(cols_b))
            same    = sorted(set(cols_a) & set(cols_b))

            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown(f"""
                <div class="kpi" style="border-top-color:#22c55e;">
                    <div class="kpi-lbl">Colunas adicionadas</div>
                    <div class="kpi-val" style="color:#22c55e;">{len(added)}</div>
                    <div class="kpi-sub">{', '.join(added) if added else 'nenhuma'}</div>
                </div>""", unsafe_allow_html=True)
            with c2:
                st.markdown(f"""
                <div class="kpi" style="border-top-color:#f97316;">
                    <div class="kpi-lbl">Colunas removidas</div>
                    <div class="kpi-val" style="color:#f97316;">{len(removed)}</div>
                    <div class="kpi-sub">{', '.join(removed) if removed else 'nenhuma'}</div>
                </div>""", unsafe_allow_html=True)
            with c3:
                st.markdown(f"""
                <div class="kpi" style="border-top-color:#3b82f6;">
                    <div class="kpi-lbl">Colunas em comum</div>
                    <div class="kpi-val" style="color:#3b82f6;">{len(same)}</div>
                    <div class="kpi-sub">schema estável</div>
                </div>""", unsafe_allow_html=True)

            # Mostra o comando equivalente em PySpark
            st.markdown(f"""
            <div class="tt-card" style="margin-top:1rem;">
                <div class="tt-title">Comando PySpark equivalente</div>
                <div class="tt-desc">
                    <code>df_v{v_a} = spark.read.format("delta").option("versionAsOf", {v_a}).load(SILVER_PATH)</code><br>
                    <code>df_v{v_b} = spark.read.format("delta").option("versionAsOf", {v_b}).load(SILVER_PATH)</code>
                </div>
            </div>""", unsafe_allow_html=True)

        # ── BLOCO 4: Partições físicas do Delta ───────────────────
        st.markdown('<div class="sec">📂 Partições Físicas — data/delta/silver/enem/</div>', unsafe_allow_html=True)
        st.caption("O Delta Lake particiona os dados por `ano` e `regiao`. Cada partição é um diretório separado com arquivos Parquet.")

        stats = load_parquet_stats()
        if not stats.empty:
            col_p1, col_p2 = st.columns(2)
            with col_p1:
                grp = stats.groupby("ano")["tamanho_kb"].sum().reset_index()
                grp.columns = ["Ano", "Tamanho Total (KB)"]
                fig_p = px.bar(grp, x="Ano", y="Tamanho Total (KB)",
                               color="Ano", color_discrete_sequence=COLORS,
                               text="Tamanho Total (KB)")
                fig_p.update_traces(texttemplate="%{text:.0f} KB", textposition="outside")
                fig_p.update_layout(**PL_DARK, height=280, showlegend=False,
                                    title="Tamanho por Ano de Aplicação")
                st.plotly_chart(fig_p, width="stretch")

            with col_p2:
                grp2 = stats.groupby(["ano","regiao"])["tamanho_kb"].sum().reset_index()
                fig_p2 = px.bar(grp2, x="regiao", y="tamanho_kb", color="ano",
                                color_discrete_sequence=COLORS, barmode="group",
                                labels={"tamanho_kb": "KB", "regiao": "Região", "ano": "Ano"})
                fig_p2.update_layout(**PL_DARK, height=280,
                                     legend=dict(bgcolor="rgba(0,0,0,0)"),
                                     title="Tamanho por Região e Ano")
                st.plotly_chart(fig_p2, width="stretch")

            st.dataframe(
                stats.groupby(["ano","regiao"]).agg(
                    arquivos=("arquivo","count"),
                    tamanho_total_kb=("tamanho_kb","sum")
                ).reset_index(),
                width="stretch", hide_index=True
            )

        # ── BLOCO 5: Referência rápida dos comandos ───────────────
        st.markdown('<div class="sec">📖 Referência de Comandos</div>', unsafe_allow_html=True)
        cmds = [
            ("VERSION AS OF",
             "Lê o estado exato em uma versão específica",
             'spark.read.format("delta").option("versionAsOf", 0).load(PATH)'),
            ("TIMESTAMP AS OF",
             "Lê o estado dos dados em uma data/hora passada",
             'spark.read.format("delta").option("timestampAsOf", "2024-01-01 00:00:00").load(PATH)'),
            ("HISTORY",
             "Auditoria completa de todas as operações",
             'DeltaTable.forPath(spark, PATH).history().show()'),
            ("RESTORE",
             "Rollback: volta a tabela para uma versão anterior",
             'DeltaTable.forPath(spark, PATH).restoreToVersion(0)'),
        ]
        for title, desc, code in cmds:
            st.markdown(f"""
            <div class="tt-card">
                <div class="tt-title">{title}</div>
                <div class="tt-desc">{desc}<br><br><code>{code}</code></div>
            </div>""", unsafe_allow_html=True)


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
        ("🟠", "INGESTÃO",   "ingestion/download.py — gera microdados ENEM simulados (estrutura real INEP)<br>Saída: data/raw/enem_{ano}.csv"),
        ("🟠", "BRONZE",     "spark_jobs/bronze.py — PySpark escreve em Delta Lake com schema enforcement<br>Particionado por ano · colunas de auditoria"),
        ("⚪", "SILVER",     "spark_jobs/silver.py — limpeza · médias · faixas · ACID MERGE<br>Schema Evolution: percentil_nacional adicionado após o merge"),
        ("🔵", "BRIDGE",     "spark_jobs/delta_to_duckdb.py — Silver Delta → Parquet → DuckDB view<br>Ponte entre o mundo Spark e o mundo dbt"),
        ("🔵", "GOLD (dbt)", "3 modelos: gld_desempenho_uf · gld_desempenho_renda · gld_evolucao_anual<br>dbt lê silver.enem via DuckDB e materializa as tabelas gold"),
        ("🟢", "TIME TRAVEL","spark_jobs/time_travel.py — VERSION AS OF · TIMESTAMP AS OF · HISTORY · ROLLBACK<br>_delta_log/ registra automaticamente cada versão"),
        ("🟣", "DASHBOARD",  "Streamlit consome gold via DuckDB read-only · 6 páginas analíticas"),
    ]

    for icon, title, desc in steps:
        st.markdown(f"""
        <div class="tt-card">
            <div class="tt-title">{icon} {title}</div>
            <div class="tt-desc">{desc}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sec">Stack</div>', unsafe_allow_html=True)
    st.dataframe(pd.DataFrame([
        ["Armazenamento",  "Delta Lake 3.2",    "Formato ACID com time travel e schema evolution"],
        ["Processamento",  "PySpark 3.5",        "Transformações, merge, particionamento"],
        ["Transformação",  "dbt-duckdb 1.8",     "Gold layer com SQL + testes de qualidade"],
        ["Query Engine",   "DuckDB 0.10",         "Lê Parquet nativamente, serve o dashboard"],
        ["Dashboard",      "Streamlit + Plotly",  "Consome gold layer via DuckDB"],
        ["CI/CD",          "GitHub Actions",      "dbt parse → run → test a cada push"],
    ], columns=["Camada", "Tecnologia", "Papel"]), width="stretch", hide_index=True)