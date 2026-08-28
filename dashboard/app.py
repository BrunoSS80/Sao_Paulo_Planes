from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GOLD_PATH = PROJECT_ROOT / "data" / "gold"
MARTS = {
    "aircraft_activity": "Aeronaves ativas",
    "airline_counts": "Companhias aéreas",
    "altitude_speed": "Altitude e velocidade",
    "monitored_area_duration": "Permanência na área",
}

st.set_page_config(
    page_title="Sao Paulo Planes | Gold Analytics",
    page_icon="✈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap');

        :root {
            --ink: #132238;
            --muted: #637083;
            --line: #dbe3ec;
            --paper: #f5f8fb;
            --card: #ffffff;
            --navy: #132d4f;
            --teal: #087f8c;
            --coral: #e56b58;
        }

        .stApp { background: var(--paper); color: var(--ink); }
        [data-testid="stSidebar"] { background: #eaf1f6; border-right: 1px solid var(--line); }
        [data-testid="stSidebar"] * { color: var(--ink); }
        h1, h2, h3 { font-family: 'Space Grotesk', sans-serif; color: var(--ink); letter-spacing: 0; }
        p, label, div { font-family: 'DM Sans', sans-serif; letter-spacing: 0; }
        .hero { border-bottom: 1px solid var(--line); padding: 1.4rem 0 1.1rem; margin-bottom: 1.2rem; }
        .eyebrow { color: var(--teal); font-size: .76rem; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; }
        .hero h1 { font-size: clamp(2rem, 4vw, 3.6rem); line-height: 1; margin: .35rem 0 .6rem; }
        .hero p { color: var(--muted); max-width: 760px; margin: 0; }
        .section-label { color: var(--navy); font-family: 'Space Grotesk', sans-serif; font-size: 1.25rem; font-weight: 700; margin: 1.7rem 0 .7rem; }
        .metric-card { background: var(--card); border: 1px solid var(--line); border-top: 4px solid var(--teal); padding: 1rem 1.1rem; min-height: 112px; }
        .metric-label { color: var(--muted); font-size: .79rem; text-transform: uppercase; letter-spacing: .05em; }
        .metric-value { color: var(--navy); font-family: 'Space Grotesk', sans-serif; font-size: 1.85rem; font-weight: 700; margin-top: .35rem; }
        .metric-help { color: var(--muted); font-size: .78rem; margin-top: .25rem; }
        .status { color: var(--muted); font-size: .82rem; margin: .2rem 0 1rem; }
        .stButton > button { border-radius: 2px; border: 1px solid var(--navy); background: var(--navy); color: white; }
        .stButton > button:hover { border-color: var(--teal); background: var(--teal); color: white; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False)
def load_mart(gold_path: str, mart_name: str) -> pd.DataFrame:
    path = Path(gold_path) / mart_name
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    return frame


def load_all_marts(gold_path: Path) -> dict[str, pd.DataFrame]:
    return {name: load_mart(str(gold_path), name) for name in MARTS}


def filtered(frame: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if frame.empty or "date" not in frame.columns:
        return frame.copy()
    return frame[frame["date"].between(start, end)].copy()


def format_number(value: float | int | None, decimals: int = 0) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def prepare_duration_chart_view(frame: pd.DataFrame) -> pd.DataFrame:
    view = frame.copy()
    if "date" in view.columns:
        view["date_label"] = pd.to_datetime(view["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    else:
        view["date_label"] = [str(value) for value in range(len(view))]
    view["duration_minutes"] = view["avg_area_duration_seconds"] / 60
    return view.sort_values("date_label").reset_index(drop=True)


def metric_card(label: str, value: str, help_text: str) -> None:
    st.markdown(
        f'<div class="metric-card"><div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div><div class="metric-help">{help_text}</div></div>',
        unsafe_allow_html=True,
    )


def empty_state(label: str, detail: str) -> None:
    st.info(f"{label}: {detail}")


def chart_layout(figure: go.Figure, height: int = 360) -> go.Figure:
    figure.update_layout(
        height=height,
        margin=dict(l=12, r=12, t=32, b=12),
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(family="DM Sans", color="#132238"),
        legend=dict(orientation="h", y=1.08, x=0),
        hoverlabel=dict(bgcolor="white"),
    )
    figure.update_xaxes(showgrid=False, linecolor="#dbe3ec")
    figure.update_yaxes(gridcolor="#edf1f5", zeroline=False)
    return figure


def main() -> None:
    st.sidebar.markdown("## Filtros")
    gold_path = Path(
        st.sidebar.text_input("Diretório da Gold", str(DEFAULT_GOLD_PATH))
    ).expanduser()
    if st.sidebar.button("Recarregar dados", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    marts = load_all_marts(gold_path)
    available_dates = sorted(
        {
            value
            for frame in marts.values()
            if not frame.empty and "date" in frame.columns
            for value in frame["date"].dropna()
        }
    )
    if not available_dates:
        st.error(f"Nenhuma data Gold foi encontrada em `{gold_path}`.")
        st.stop()

    min_date, max_date = available_dates[0], available_dates[-1]
    selected_range = st.sidebar.date_input(
        "Período (UTC)", value=(min_date, max_date), min_value=min_date, max_value=max_date
    )
    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        start_date, end_date = selected_range
    else:
        start_date = end_date = selected_range

    data = {name: filtered(frame, start_date, end_date) for name, frame in marts.items()}
    loaded = [name for name, frame in data.items() if not frame.empty]
    st.markdown(
        '<div class="hero"><div class="eyebrow">Sao Paulo Planes / Gold Analytics</div>'
        '<h1>Leitura do tráfego aéreo</h1>'
        '<p>Visão executiva das aeronaves observadas na região de São Paulo. As métricas representam observações e sessões monitoradas, não voos.</p></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="status">Período: <strong>{start_date.strftime("%d/%m/%Y")}</strong> a '
        f'<strong>{end_date.strftime("%d/%m/%Y")}</strong> · {len(loaded)} de {len(MARTS)} marts disponíveis</div>',
        unsafe_allow_html=True,
    )

    activity = data["aircraft_activity"]
    altitude = data["altitude_speed"]
    duration = data["monitored_area_duration"]
    total_observed = (
        int(activity["active_aircraft_count"].sum()) if not activity.empty else None
    )
    peak_active = (
        int(activity["active_aircraft_count"].max()) if not activity.empty else None
    )
    avg_altitude = float(altitude["avg_altitude"].mean()) if not altitude.empty else None
    avg_velocity = float(altitude["avg_velocity"].mean()) if not altitude.empty else None
    avg_duration = (
        float(duration["avg_area_duration_seconds"].mean() / 60)
        if not duration.empty
        else None
    )

    st.markdown('<div class="section-label">Resumo do período</div>', unsafe_allow_html=True)
    cards = st.columns(5)
    with cards[0]:
        metric_card("Observações horárias", format_number(total_observed), "soma das aeronaves observadas")
    with cards[1]:
        metric_card("Pico de aeronaves", format_number(peak_active), "maior contagem em uma hora")
    with cards[2]:
        metric_card("Altitude média", f"{format_number(avg_altitude, 0)} m", "barométrica")
    with cards[3]:
        metric_card("Velocidade média", f"{format_number(avg_velocity, 1)} m/s", "observações válidas")
    with cards[4]:
        metric_card("Permanência média", f"{format_number(avg_duration, 1)} min", "sessões encerradas")

    st.markdown('<div class="section-label">Movimento aéreo</div>', unsafe_allow_html=True)
    movement_left, movement_right = st.columns(2)
    with movement_left:
        if activity.empty:
            empty_state("Aeronaves ativas", "não há dados no período selecionado.")
        else:
            activity_chart = px.area(
                activity.sort_values(["date", "hour"]),
                x="hour",
                y="active_aircraft_count",
                color="date",
                markers=True,
                labels={"hour": "Hora UTC", "active_aircraft_count": "Aeronaves", "date": "Data"},
                color_discrete_sequence=["#087f8c", "#e56b58", "#315c8a", "#d39b3d"],
            )
            st.plotly_chart(chart_layout(activity_chart), use_container_width=True)
    with movement_right:
        if altitude.empty:
            empty_state("Altitude e velocidade", "não há dados no período selecionado.")
        else:
            measures = altitude.sort_values(["date", "hour"]).copy()
            measures["timestamp"] = pd.to_datetime(measures["date"].astype(str)) + pd.to_timedelta(measures["hour"], unit="h")
            measure_chart = go.Figure()
            measure_chart.add_trace(go.Scatter(x=measures["timestamp"], y=measures["avg_altitude"], name="Altitude (m)", mode="lines+markers", line=dict(color="#132d4f", width=3)))
            measure_chart.add_trace(go.Scatter(x=measures["timestamp"], y=measures["avg_velocity"], name="Velocidade (m/s)", mode="lines+markers", line=dict(color="#e56b58", width=3), yaxis="y2"))
            measure_chart.update_layout(yaxis=dict(title="Altitude (m)"), yaxis2=dict(title="Velocidade (m/s)", overlaying="y", side="right"), xaxis_title="Horário UTC")
            st.plotly_chart(chart_layout(measure_chart), use_container_width=True)

    st.markdown('<div class="section-label">Companhias observadas</div>', unsafe_allow_html=True)
    airlines = data["airline_counts"]
    if airlines.empty:
        empty_state("Companhias aéreas", "o mart não está disponível ou não possui dados.")
    else:
        ranking = airlines.groupby("airline_name", as_index=False)["aircraft_count"].sum().sort_values("aircraft_count", ascending=True).tail(12)
        airline_chart = px.bar(ranking, x="aircraft_count", y="airline_name", orientation="h", labels={"aircraft_count": "Aeronaves observadas", "airline_name": "Companhia"}, color_discrete_sequence=["#087f8c"])
        st.plotly_chart(chart_layout(airline_chart, 390), use_container_width=True)

    st.markdown('<div class="section-label">Permanência na área monitorada</div>', unsafe_allow_html=True)
    if duration.empty:
        empty_state("Duração das sessões", "não há sessões encerradas no período selecionado.")
    else:
        duration_view = prepare_duration_chart_view(duration)
        duration_chart = px.bar(
            duration_view,
            x="date_label",
            y="duration_minutes",
            category_orders={"date_label": list(duration_view["date_label"])},
            labels={"date_label": "Data", "duration_minutes": "Minutos médios"},
            color_discrete_sequence=["#e56b58"],
            hover_data={"sessions_used": True, "duration_minutes": ":.1f"},
        )
        st.plotly_chart(chart_layout(duration_chart, 320), use_container_width=True)

    with st.expander("Inspecionar dados Gold"):
        for name, frame in data.items():
            st.markdown(f"**{MARTS[name]}**")
            if frame.empty:
                st.caption("Sem dados para o período selecionado.")
            else:
                st.dataframe(frame, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
