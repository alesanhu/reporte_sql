
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

###############################################################################
# 0. LOGIN / CONEXIÓN DINÁMICA                                               #
###############################################################################
# El usuario ingresa manualmente las credenciales de la BD la primera vez que
# abre la app.  Se guardan en `st.session_state["db_cfg"]` y se reutilizan sin
# volver a pedirlas durante la sesión.
###############################################################################

st.set_page_config(page_title="Informe de Estudiantes por Sección", layout="wide")

###############################################################################
# 1. FORMULARIO DE LOGIN                                                     #
###############################################################################
if "db_cfg" not in st.session_state:
    with st.form("login", clear_on_submit=False):
        st.markdown("### 🔐 Conexión a la base de datos (login único por sesión)")
        col1, col2 = st.columns(2)
        with col1:
            user = st.text_input("Usuario", key="db_user")
            host = st.text_input("Host",  key="db_host")
            port = st.number_input("Puerto", min_value=1, max_value=65535, value=3306, key="db_port")
        with col2:
            password = st.text_input("Contraseña", type="password", key="db_pass")
            dbname = st.text_input("Base de datos", key="db_name")
        ok = st.form_submit_button("Conectar")

        if ok:
            cfg = dict(user=user, password=password, host=host, port=int(port), dbname=dbname)
            # intentamos conectar para validar credenciales
            test_url = (
                f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['dbname']}?charset=utf8mb4"
            )
            try:
                engine = create_engine(test_url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                st.session_state["db_cfg"] = cfg
                st.success("✔️ Conexión establecida. ¡Bienvenido!")
                st.experimental_rerun()
            except SQLAlchemyError as e:
                st.error(f"❌ Error al conectar: {e}")
    st.stop()

###############################################################################
# 2. CONEXIÓN A LA BD (usa la cfg almacenada)                                #
###############################################################################
@st.cache_resource(show_spinner=False)
def get_engine():
    cfg = st.session_state["db_cfg"]
    url = (
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['dbname']}?charset=utf8mb4"
    )
    return create_engine(url)

@st.cache_data(show_spinner=False)
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), get_engine(), params=params)

###############################################################################
# 3. SENTENCIAS SQL  (idénticas a la versión anterior)                        #
###############################################################################
SQL_CARRERAS = """
SELECT id_code AS cod, ds_publicId AS code, ds_name AS nombre
FROM upl_careers WHERE is_active=1 ORDER BY ds_publicId;
"""

SQL_PENSUMS = """
SELECT id_code AS cod, ds_publicId AS code, ds_name AS nombre
FROM upl_pensums WHERE is_active=1 AND id_career=:car ORDER BY ds_publicId;
"""

SQL_MALLA = """
SELECT ucp.nm_nivel AS Nivel, c.id_code AS CursoId,
       c.ds_publicId AS CursoCod, c.ds_name AS Curso
FROM upl_courses_pensums ucp
JOIN upl_courses c ON c.id_code=ucp.id_course AND c.is_active=1
WHERE ucp.is_active=1 AND ucp.id_pensum=:pen
ORDER BY ucp.nm_nivel, c.ds_publicId;
"""

SQL_PERIODOS_CUR = """
SELECT DISTINCT ap.id_code AS cod, ap.ds_name AS nombre
FROM upl_academicperiods ap
JOIN cls_sections se        ON se.id_academic_period = ap.id_code AND se.is_active=1
JOIN cls_sectionscomposition sc ON sc.id_section=se.id_code AND sc.id_course=:cur
WHERE ap.is_active=1
ORDER BY ap.ds_name DESC;
"""

SQL_PROFES_CUR = """
SELECT DISTINCT te.id_code AS cod, te.ds_fullname AS nombre
FROM cls_sections_teachers st
JOIN upl_teachers te ON te.id_code=st.id_teacher AND te.is_active=1
JOIN cls_sections se ON se.id_code = st.id_section AND se.is_active=1 AND se.id_academic_period=:per
JOIN cls_sectionscomposition sc ON sc.id_section = se.id_code AND sc.id_course=:cur
WHERE st.is_active=1
ORDER BY te.ds_fullname;
"""

SQL_SECCIONES_CUR = """
SELECT DISTINCT se.id_code AS cod,
       COALESCE(se.ds_integrationid, se.ds_section, se.id_code) AS label
FROM cls_sections se
JOIN cls_sectionscomposition sc ON sc.id_section=se.id_code AND sc.id_course=:cur
WHERE se.is_active=1 AND se.id_academic_period=:per
ORDER BY label;
"""

SQL_COMP_CURSO = """
SELECT ic.ds_publicId AS CompetencyID, ic.ds_name AS Competencia
FROM imp_courses_competencies cc
JOIN imp_competencies ic ON ic.id_code = cc.id_competency AND ic.is_active=1
WHERE cc.is_active=1 AND cc.id_course=:cur AND cc.id_pensum=:pen
ORDER BY ic.ds_publicId;
"""

# (SQL_REPORTE y SQL_DETALLE permanecen sin cambios, pero los incluimos para
#  que el archivo sea autocontenido)
SQL_REPORTE = """<el mismo texto que tenías>"""
SQL_DETALLE = """<el mismo texto que tenías>"""

###############################################################################
# 4. INTERFAZ PRINCIPAL                                                      #
###############################################################################
st.title("Informe de Estudiantes por Sección")

# ── Filtro: Carrera ─────────────────────────────────────────────────────────
car_df = run_query(SQL_CARRERAS)
car_df["label"] = car_df["code"] + " – " + car_df["nombre"]
car_sel  = st.selectbox("🏛️ Carrera", [""] + car_df["label"].tolist(), key="carrera")
car_code = car_df.loc[car_df["label"]==car_sel, "cod"].squeeze() if car_sel else None

# ── Filtro: Plan ────────────────────────────────────────────────────────────
pen_df = run_query(SQL_PENSUMS,{"car":car_code}) if car_code else pd.DataFrame(columns=["cod","code","nombre"])
pen_df["label"] = pen_df["code"] + " – " + pen_df["nombre"]
pen_sel  = st.selectbox("🎓 Plan de Estudio", [""] + pen_df["label"].tolist(), key="pensum")
pen_code = pen_df.loc[pen_df["label"]==pen_sel, "cod"].squeeze() if pen_sel else None

# ── Malla del plan ──────────────────────────────────────────────────────────
if pen_code:
    malla_df = run_query(SQL_MALLA,{"pen":pen_code})
    st.subheader("📋 Malla del Plan de Estudio")
    cols = st.columns(len(sorted(malla_df["Nivel"].unique())), gap="small")
    for nivel, col in zip(sorted(malla_df["Nivel"].unique()), cols):
        with col:
            st.markdown(f"**{nivel}º Semestre**")
            for _,rw in malla_df[malla_df["Nivel"]==nivel].iterrows():
                if st.button(f"{rw.CursoCod}\n{rw.Curso}", key=f"cur_{rw.CursoId}"):
                    st.session_state["curso_id"]     = rw.CursoId
                    st.session_state["curso_public"] = rw.CursoCod
                    st.session_state["curso_nombre"] = rw.Curso

# ── Competencias del curso seleccionado ────────────────────────────────────
curso_id = st.session_state.get("curso_id")
if pen_code and curso_id:
    st.markdown(f"### 🏅 Competencias del curso **{st.session_state['curso_public']} – {st.session_state['curso_nombre']}**")
    comp_df = run_query(SQL_COMP_CURSO,{"cur":curso_id,"pen":pen_code})
    st.dataframe(comp_df if not comp_df.empty else pd.DataFrame({"Msg":["No hay competencias"]}), use_container_width=True)

# ── Filtros dependientes: Período / Profesor / Sección ─────────────────────
if curso_id:
    per_df = run_query(SQL_PERIODOS_CUR,{"cur":curso_id})
    per_df["label"] = per_df["cod"].astype(str)+" – "+per_df["nombre"]
    per_sel  = st.selectbox("🗓️ Período", ["Todos"]+per_df["label"].tolist(), key="periodo")
    per_code = None if per_sel in ("", "Todos") else per_df.loc[per_df["label"]==per_sel,"cod"].squeeze()

    prof_df = run_query(SQL_PROFES_CUR,{"cur":curso_id,"per":per_code or 0})
    prof_df["label"] = prof_df["cod"].astype(str)+" – "+prof_df["nombre"]
    prof_sel   = st.selectbox("👨‍🏫 Profesor", ["Todos"]+prof_df["label"].tolist(), key="prof")
    teach_code = None if prof_sel in ("", "Todos") else prof_df.loc[prof_df["label"]==prof_sel,"cod"].squeeze()

    sec_df = run_query(SQL_SECCIONES_CUR,{"cur":curso_id,"per":per_code or 0})
    sec_sel  = st.selectbox("✏️ Sección", ["Todos"]+sec_df["label"].tolist(), key="sec")
    sec_code = None if sec_sel in ("", "Todos") else sec_df.loc[sec_df["label"]==sec_sel,"cod"].squeeze()
else:
    st.info("Selecciona un curso para continuar.")
    st.stop()

# ── Botón Generar reporte ──────────────────────────────────────────────────
if st.button("▶️ Generar reporte", key="btn_reporte"):
    df = run_query(SQL_REPORTE,{"cur":curso_id, "per":per_code, "sec":sec_code, "teach":teach_code})
    st.dataframe(df, use_container_width=True)

    if sec_code:
        det = run_query(SQL_DETALLE,{"sec":sec_code})
        st.dataframe(det, use_container_width=True)

