import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

"""
───────────────────────────────────────────────────────────────────────────────
INFORME DE ESTUDIANTES POR SECCIÓN  –  versión final
───────────────────────────────────────────────────────────────────────────────
• Inicio con formulario de conexión (login único por sesión)
• Conexión guardada en `st.session_state["db_cfg"]`
• Flujo de filtros jerárquico y dependiente del **curso** elegido en la malla
    Carrera → Pensum → Curso → Período → Profesor → Sección
    («Todos» ≡ sin filtrar para cada combo)
• Un solo botón  «Generar reporte»
• Compatibilidad: Python ≤ 3.12  ·  SQLAlchemy 2.0.30  ·  pymysql
"""
###############################################################################
# 0. Config de página y LOGIN dinámico                                        #
###############################################################################
st.set_page_config(page_title="Informe de Estudiantes por Sección", layout="wide")

if "db_cfg" not in st.session_state:
    with st.form("login", clear_on_submit=False):
        st.markdown("### 🔐 Conexión a la base de datos (login único por sesión)")
        c1, c2 = st.columns(2)
        with c1:
            user = st.text_input("Usuario")
            host = st.text_input("Host")
            port = st.number_input("Puerto", 1, 65535, 3306)
        with c2:
            password = st.text_input("Contraseña", type="password")
            dbname = st.text_input("Base de datos")
        ok = st.form_submit_button("Conectar")
        if ok:
            cfg = dict(user=user, password=password, host=host, port=int(port), dbname=dbname)
            url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}?charset=utf8mb4"
            try:
                engine = create_engine(url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                st.session_state["db_cfg"] = cfg
                st.success("✔️ Conexión establecida, ¡bienvenido!")
                st.experimental_rerun()
            except SQLAlchemyError as e:
                st.error(f"❌ Error al conectar: {e}")
    st.stop()

###############################################################################
# 1. Conexión y helpers                                                       #
###############################################################################
@st.cache_resource(show_spinner=False)
def get_engine():
    cfg = st.session_state["db_cfg"]
    url = (
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@"
        f"{cfg['host']}:{cfg['port']}/{cfg['dbname']}?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True)

@st.cache_data(show_spinner=False)
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), get_engine(), params=params)

###############################################################################
# 2. CONSULTAS SQL                                                            #
###############################################################################
SQL_CARRERAS = """
SELECT id_code AS cod, ds_publicId AS code, ds_name AS nombre
FROM upl_careers WHERE is_active=1 ORDER BY ds_publicId;"""

SQL_PENSUMS = """
SELECT id_code AS cod, ds_publicId AS code, ds_name AS nombre
FROM upl_pensums WHERE is_active=1 AND id_career=:car ORDER BY ds_publicId;"""

SQL_MALLA = """
SELECT ucp.nm_nivel AS Nivel, c.id_code AS CursoId,
       c.ds_publicId AS CursoCod, c.ds_name AS Curso
FROM upl_courses_pensums ucp
JOIN upl_courses c ON c.id_code=ucp.id_course AND c.is_active=1
WHERE ucp.is_active=1 AND ucp.id_pensum=:pen
ORDER BY ucp.nm_nivel, c.ds_publicId;"""

SQL_PERIODOS_CUR = """
SELECT DISTINCT ap.id_code AS cod, ap.ds_name AS nombre
FROM upl_academicperiods ap
JOIN cls_sections se              ON se.id_academic_period = ap.id_code AND se.is_active=1
JOIN cls_sectionscomposition sc   ON sc.id_section = se.id_code AND sc.id_course=:cur
WHERE ap.is_active=1
ORDER BY ap.ds_name DESC;"""

SQL_PROFES_CUR = """
SELECT DISTINCT te.id_code AS cod, te.ds_fullname AS nombre
FROM cls_sections_teachers st
JOIN upl_teachers te ON te.id_code = st.id_teacher AND te.is_active=1
JOIN cls_sections se ON se.id_code = st.id_section AND se.is_active=1 AND se.id_academic_period=:per
JOIN cls_sectionscomposition sc ON sc.id_section = se.id_code AND sc.id_course=:cur
WHERE st.is_active=1
ORDER BY te.ds_fullname;"""

SQL_SECCIONES_CUR = """
SELECT DISTINCT se.id_code AS cod,
       COALESCE(se.ds_integrationid,se.ds_section,se.id_code) AS label
FROM cls_sections se
JOIN cls_sectionscomposition sc ON sc.id_section = se.id_code AND sc.id_course=:cur
WHERE se.is_active=1 AND se.id_academic_period=:per
  AND (:teach IS NULL OR EXISTS (
        SELECT 1 FROM cls_sections_teachers st2
         WHERE st2.id_section = se.id_code AND st2.id_teacher = :teach AND st2.is_active=1))
ORDER BY label;"""

SQL_COMP_CURSO = """
SELECT ic.ds_publicId AS CompetencyID, ic.ds_name AS Competencia
FROM imp_courses_competencies cc
JOIN imp_competencies ic ON ic.id_code = cc.id_competency AND ic.is_active=1
WHERE cc.is_active=1 AND cc.id_course=:cur AND cc.id_pensum=:pen
ORDER BY ic.ds_publicId;"""

SQL_REPORTE = """
WITH x_out AS (
  SELECT so.id_section,
         COUNT(DISTINCT so.id_code) AS Outcomes,
         COUNT(DISTINCT cl.id_code) AS Competencias
    FROM imp_section_outcomes so
    JOIN imp_competencylevel_sectionoutcomes clso ON clso.id_section_outcome = so.id_code AND clso.is_active=1
    JOIN imp_competency_levels cl ON cl.id_code = clso.id_competency_level AND cl.is_active=1
   WHERE so.is_active=1
   GROUP BY so.id_section
),
x_notes AS (
  SELECT asm.id_section, 1 AS TieneNotas
    FROM asm_student_marks asm
   WHERE asm.is_active=1
   GROUP BY asm.id_section
),
y_eval AS (
  SELECT ec.id_section, 1 AS EvalConOut
    FROM imp_section_evaluationcomponents ec
    JOIN imp_sectioncomponent_outcomes sco ON sco.id_section_evaluationcomponent = ec.id_code AND sco.is_active=1
   WHERE ec.is_active=1
   GROUP BY ec.id_section
)
SELECT ca.ds_publicId  AS CarreraCod,
       ca.ds_name      AS Carrera,
       pe.ds_publicId  AS PlanCod,
       pe.ds_name      AS Plan,
       ap.ds_name      AS Periodo,
       co.ds_publicId  AS CursoCod,
       co.ds_name      AS Curso,
       COALESCE(se.ds_integrationid,se.ds_section,se.id_code) AS Seccion,
       IFNULL(x_out.Outcomes,0)      AS Outcomes,
       IFNULL(x_out.Competencias,0)  AS Competencias,
       CASE WHEN y_eval.EvalConOut=1 THEN 'Sí' ELSE 'No' END AS EvalConOut,
       IFNULL(x_notes.TieneNotas,0)   AS NotasActivas,
       COUNT(DISTINCT ss.id_student)  AS Estudiantes,
       GROUP_CONCAT(DISTINCT te.ds_fullname ORDER BY te.ds_fullname SEPARATOR '; ') AS Profesores
FROM cls_students_sections ss
JOIN cls_sections se ON se.id_code = ss.id_section AND se.is_active=1
JOIN cls_sections_teachers st ON st.id_section = se.id_code AND st.is_active=1
JOIN upl_teachers te ON te.id_code = st.id_teacher AND te.is_active=1
JOIN cls_sectionscomposition sc ON sc.id_section = se.id_code AND sc.is_active=1
JOIN upl_courses co ON co.id_code = sc.id_course AND co.is_active=1
JOIN upl_pensums pe ON pe.id_code = sc.id_pensum AND pe.is_active=1
JOIN upl_careers ca ON ca.id_code = pe.id_career AND ca.is_active=1
JOIN upl_academicperiods ap ON ap.id_code = se.id_academic_period AND ap.is_active=1
LEFT JOIN x_out   ON x_out.id_section = se.id_code
LEFT JOIN x_notes ON x_notes.id_section = se.id_code
LEFT JOIN y_eval  ON y_eval.id_section = se.id_code
WHERE (:car   IS NULL OR ca.id_code = :car)
  AND (:pen   IS NULL OR pe.id_code = :pen)
  AND (:per   IS NULL OR ap.id_code = :per)
  AND (:sec   IS NULL OR se.id_code = :sec)
  AND (:teach IS NULL OR te.id_code = :teach)
  AND (:cur   IS NULL OR co.id_code = :cur)
  AND ss.is_active=1
GROUP BY CarreraCod,Carrera,PlanCod,Plan,Periodo,CursoCod,Curso,Seccion,Outcomes,Competencias,EvalConOut,NotasActivas
ORDER BY CursoCod,Seccion;"""

SQL_DETALLE = """
SELECT ic.ds_publicId AS CompetencyID,
       ic.ds_name     AS Competencia,
       so.id_code     AS OutcomeID,
       so.ds_name     AS Outcome,
       COUNT(DISTINCT sco.id_code) AS `Nº EvalComponents`
FROM imp_section_outcomes so
LEFT JOIN imp_sectioncomponent_outcomes sco ON sco.id_section_outcome = so.id_code AND sco.is_active=1
JOIN imp_competencylevel_sectionoutcomes clso ON clso.id_section_outcome = so.id_code AND clso.is_active=1
JOIN imp_competency_levels cl ON cl.id_code = clso.id_competency_level AND cl.is_active=1
JOIN imp_competencies ic ON ic.id_code = cl.id_competency AND ic.is_active=1
WHERE so.is_active=1 AND so.id_section=:sec
GROUP BY CompetencyID,Competencia,OutcomeID,Outcome
ORDER BY Competencia,Outcome;"""

###############################################################################
# 3. INTERFAZ PRINCIPAL                                                      #
###############################################################################
st.title("Informe de Estudiantes por Sección")

# — Carrera ------------------------------------------------------------------
car_df = run_query(SQL_CARRERAS)
car_df["label"] = car_df["code"] + " – " + car_df["nombre"]
car_sel = st.selectbox("🏛️ Carrera", ["Todos"] + car_df["label"].tolist(), key="f_car")
car_code = None if car_sel == "Todos" else car_df.loc[car_df["label"]==car_sel, "cod"].squeeze()

# — Pensum -------------------------------------------------------------------
pen_df = run_query(SQL_PENSUMS, {"car":car_code}) if car_code else pd.DataFrame()
pen_df["label"] = pen_df["code"] + " – " + pen_df["nombre"]
pen_sel = st.selectbox("🎓 Plan de Estudio", ["Todos"] + pen_df["label"].tolist(), key="f_pen")
pen_code = None if pen_sel == "Todos" else pen_df.loc[pen_df["label"]==pen_sel, "cod"].squeeze()

# — Malla y elección de curso -----------------------------------------------
curso_id = None
if pen_code:
    malla_df = run_query(SQL_MALLA, {"pen":pen_code})
    st.subheader("📋 Malla del Plan de Estudio")
    cols = st.columns(len(sorted(malla_df["Nivel"].unique())), gap="small")
    for i, nivel in enumerate(sorted(malla_df["Nivel"].unique())):
        with cols[i]:
            st.markdown(f"**{nivel}º Semestre**")
            for _, row in malla_df[malla_df["Nivel"]==nivel].iterrows():
                lbl = f"**{row.CursoCod}**  \n{row.Curso}"
                if st.button(lbl, key=f"c_{row.CursoId}"):
                    st.session_state.update({
                        "curso_id": row.CursoId,
                        "curso_public": row.CursoCod,
                        "curso_nombre": row.Curso
                    })
curso_id      = st.session_state.get("curso_id")
curso_public  = st.session_state.get("curso_public")
curso_nombre  = st.session_state.get("curso_nombre")

# — Competencias del curso ----------------------------------------------------
if curso_id and pen_code:
    st.markdown(f"### 🏅 Competencias del curso **{curso_public} – {curso_nombre}**")
    comp_df = run_query(SQL_COMP_CURSO, {"cur":curso_id, "pen":pen_code})
    st.dataframe(comp_df, use_container_width=True) if not comp_df.empty else st.info("Sin competencias definidas")

# — Filtros dependientes del curso -------------------------------------------
if curso_id:
    per_df = run_query(SQL_PERIODOS_CUR, {"cur":curso_id})
    per_df["label"] = per_df["nombre"]
    per_sel = st.selectbox("📅 Período", ["Todos"] + per_df["label"].tolist(), key="f_per")
    per_code = None if per_sel == "Todos" else per_df.loc[per_df["label"]==per_sel, "cod"].squeeze()

    prof_df = run_query(SQL_PROFES_CUR, {"cur":curso_id, "per":per_code})
    prof_df["label"] = prof_df["nombre"]
    prof_sel = st.selectbox("👩‍🏫 Profesor", ["Todos"] + prof_df["label"].tolist(), key="f_prof")
    teach_code = None if prof_sel == "Todos" else prof_df.loc[prof_df["label"]==prof_sel, "cod"].squeeze()

    sec_df = run_query(SQL_SECCIONES_CUR, {"cur":curso_id, "per":per_code, "teach":teach_code})
    sec_sel = st.selectbox("✏️ Sección", ["Todos"] + sec_df["label"].tolist(), key="f_sec")
    sec_code = None if sec_sel == "Todos" else sec_df.loc[sec_df["label"]==sec_sel, "cod"].squeeze()
else:
    per_code = teach_code = sec_code = None

###############################################################################
# 4. BOTÓN Y RESULTADOS                                                       #
###############################################################################

gen = st.button("▶️ Generar reporte", disabled= curso_id is None, key="btn_run")

if gen and curso_id:
    # --- Tabla principal ----------------------------------------------------
    df = run_query(SQL_REPORTE, {
        "car": car_code, "pen": pen_code, "per": per_code,
        "sec": sec_code, "teach": teach_code, "cur": curso_id
    })
    st.dataframe(df, use_container_width=True)

    # --- Detalle competencias/outcomes -------------------------------------
    if sec_code:  # solo cuando hay sección específica
        det = run_query(SQL_DETALLE, {"sec":sec_code})
        st.subheader("🔍 Detalle de Competencias & Outcomes")
        st.dataframe(det, use_container_width=True)
