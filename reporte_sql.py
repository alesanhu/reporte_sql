import streamlit as st
import pandas as pd
import sqlalchemy 
from sqlalchemy import create_engine, text

###############################################################################
# 0. CONFIGURACIÓN GENERAL                                                   #
###############################################################################
# La app se despliega en Streamlit Community Cloud. Las credenciales de
# base de datos se guardan en la pestaña *Secrets* con la clave «mariadb»:
#
# [mariadb]
# user     = "mi_usuario"
# password = "mi_pass"
# host     = "midb.ejemplo.com"
# port     = 3306
# dbname   = "mi_base"
###############################################################################

st.set_page_config(page_title="Informe de Estudiantes por Sección", layout="wide")

###############################################################################
# 1. CONEXIÓN A LA BD                                                         #
###############################################################################
@st.cache_resource(show_spinner=False)
def get_engine():
    cfg = st.secrets["mariadb"]
    url = (
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg.get('port',3306)}/{cfg['dbname']}?charset=utf8mb4"
    )
    return create_engine(url)

@st.cache_data(show_spinner=False)
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), get_engine(), params=params)

###############################################################################
# 2. SENTENCIAS SQL                                                           #
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

# --- filtros dependientes del curso seleccionado ---------------------------
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

SQL_REPORTE = """
WITH x_out AS (
  SELECT so.id_section,
         COUNT(DISTINCT so.id_code) AS Outcomes,
         COUNT(DISTINCT cl.id_code) AS Competencias
  FROM imp_section_outcomes so
  JOIN imp_competencylevel_sectionoutcomes clso ON clso.id_section_outcome=so.id_code AND clso.is_active=1
  JOIN imp_competency_levels cl ON cl.id_code=clso.id_competency_level AND cl.is_active=1
  WHERE so.is_active=1
  GROUP BY so.id_section
),
 x_notes AS (
  SELECT asm.id_section, 1 AS TieneNotas
  FROM asm_student_marks asm WHERE asm.is_active=1
  GROUP BY asm.id_section
),
 y_eval AS (
  SELECT ec.id_section, 1 AS EvalConOut
  FROM imp_section_evaluationcomponents ec
  JOIN imp_sectioncomponent_outcomes sco ON sco.id_section_evaluationcomponent=ec.id_code AND sco.is_active=1
  WHERE ec.is_active=1 GROUP BY ec.id_section
)
SELECT ca.ds_publicId  AS CarreraCod, ca.ds_name AS Carrera,
       pe.ds_publicId  AS PlanCod,  pe.ds_name  AS Plan,
       ap.ds_name      AS Periodo,
       co.ds_publicId  AS CursoCod, co.ds_name  AS Curso,
       COALESCE(se.ds_integrationid,se.ds_section,se.id_code) AS Seccion,
       IFNULL(x_out.Outcomes,0)     AS Outcomes,
       IFNULL(x_out.Competencias,0) AS Competencias,
       CASE WHEN y_eval.EvalConOut=1 THEN 'Sí' ELSE 'No' END AS EvalConOut,
       IFNULL(x_notes.TieneNotas,0) AS NotasActivas,
       COUNT(DISTINCT ss.id_student) AS Estudiantes,
       GROUP_CONCAT(DISTINCT te.ds_fullname ORDER BY te.ds_fullname SEPARATOR '; ') AS Profesores
FROM cls_students_sections ss
JOIN cls_sections se             ON se.id_code = ss.id_section AND se.is_active=1
JOIN cls_sections_teachers st     ON st.id_section = se.id_code AND st.is_active=1
JOIN upl_teachers te              ON te.id_code = st.id_teacher AND te.is_active=1
JOIN cls_sectionscomposition sc   ON sc.id_section = se.id_code AND sc.id_course = :cur
JOIN upl_courses co               ON co.id_code = sc.id_course AND co.is_active=1
JOIN upl_pensums pe               ON pe.id_code = sc.id_pensum AND pe.is_active=1
JOIN upl_careers ca               ON ca.id_code = pe.id_career AND ca.is_active=1
JOIN upl_academicperiods ap       ON ap.id_code = se.id_academic_period AND ap.is_active=1
LEFT JOIN x_out   ON x_out.id_section = se.id_code
LEFT JOIN x_notes ON x_notes.id_section = se.id_code
LEFT JOIN y_eval  ON y_eval.id_section = se.id_code
WHERE (:per   IS NULL OR ap.id_code = :per)
  AND (:sec   IS NULL OR se.id_code = :sec)
  AND (:teach IS NULL OR te.id_code = :teach)
GROUP BY CarreraCod,Carrera,PlanCod,Plan,Periodo,CursoCod,Curso,Seccion,Outcomes,Competencias,EvalConOut,NotasActivas
ORDER BY CursoCod,Seccion;
"""

SQL_DETALLE = """
SELECT ic.ds_publicId AS CompetencyID, ic.ds_name AS Competencia,
       so.id_code     AS OutcomeID,   so.ds_name  AS Outcome,
       COUNT(DISTINCT sco.id_code)    AS `Nº EvalComponents`
FROM imp_section_outcomes so
LEFT JOIN imp_sectioncomponent_outcomes sco ON sco.id_section_outcome = so.id_code AND sco.is_active=1
JOIN imp_competencylevel_sectionoutcomes clso ON clso.id_section_outcome=so.id_code AND clso.is_active=1
JOIN imp_competency_levels cl ON cl.id_code=clso.id_competency_level AND cl.is_active=1
JOIN imp_competencies ic      ON ic.id_code=cl.id_competency AND ic.is_active=1
WHERE so.is_active=1 AND so.id_section=:sec
GROUP BY so.id_code, ic.ds_publicId, ic.ds_name, so.ds_name
ORDER BY ic.ds_name, so.ds_name;
"""

###############################################################################
# 3. INTERFAZ STREAMLIT                                                       #
###############################################################################
st.title("Informe de Estudiantes por Sección")

# ── Filtro 1: Carrera ────────────────────────────────────────────────────────
car_df = run_query(SQL_CARRERAS)
car_df["label"] = car_df["code"] + " – " + car_df["nombre"]
car_sel  = st.selectbox("🏛️ Carrera", [""] + car_df["label"].tolist(), key="carrera")
car_code = car_df.loc[car_df["label"]==car_sel, "cod"].squeeze() if car_sel else None

# ── Filtro 2: Plan ───────────────────────────────────────────────────────────
pen_df = run_query(SQL_PENSUMS,{"car":car_code}) if car_code else pd.DataFrame(columns=["cod","code","nombre"])
pen_df["label"] = pen_df["code"] + " – " + pen_df["nombre"]
pen_sel  = st.selectbox("🎓 Plan de Estudio", [""] + pen_df["label"].tolist(), key="pensum")
pen_code = pen_df.loc[pen_df["label"]==pen_sel, "cod"].squeeze() if pen_sel else None

# ── Malla del plan ───────────────────────────────────────────────────────────
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

# ── Competencias del curso seleccionado ─────────────────────────────────────
curso_id = st.session_state.get("curso_id")
if pen_code and curso_id:
    st.markdown(f"### 🏅 Competencias del curso **{st.session_state['curso_public']} – {st.session_state['curso_nombre']}**")
    comp_df = run_query(SQL_COMP_CURSO,{"cur":curso_id,"pen":pen_code})
    st.dataframe(comp_df if not comp_df.empty else pd.DataFrame({"Msg":["No hay competencias"]}), use_container_width=True)

# ── Filtro 3: Período (depende del curso) ────────────────────────────────────
if curso_id:
    per_df = run_query(SQL_PERIODOS_CUR,{"cur":curso_id})
    per_df["label"] = per_df["cod"].astype(str)+" – "+per_df["nombre"]
    per_sel  = st.selectbox("🗓️ Período", ["Todos"]+per_df["label"].tolist(), key="periodo")
    per_code = None if per_sel in ("", "Todos") else per_df.loc[per_df["label"]==per_sel,"cod"].squeeze()

    # Profesor
    prof_df = run_query(SQL_PROFES_CUR,{"cur":curso_id,"per":per_code or 0})
    prof_df["label"] = prof_df["cod"].astype(str)+" – "+prof_df["nombre"]
    prof_sel   = st.selectbox("👨‍🏫 Profesor", ["Todos"]+prof_df["label"].tolist(), key="prof")
    teach_code = None if prof_sel in ("", "Todos") else prof_df.loc[prof_df["label"]==prof_sel,"cod"].squeeze()

    # Sección
    sec_df = run_query(SQL_SECCIONES_CUR,{"cur":curso_id,"per":per_code or 0})
    sec_sel  = st.selectbox("✏️ Sección", ["Todos"]+sec_df["label"].tolist(), key="sec")
    sec_code = None if sec_sel in ("", "Todos") else sec_df.loc[sec_df["label"]==sec_sel,"cod"].squeeze()
else:
    st.info("Selecciona primero un curso en la malla para habilitar los filtros inferiores.")
    st.stop()

# ── Botón generar ───────────────────────────────────────────────────────────
if st.button("▶️ Generar reporte", key="btn_reporte"):
    df = run_query(SQL_REPORTE,{"cur":curso_id, "per":per_code, "sec":sec_code, "teach":teach_code})
    st.dataframe(df, use_container_width=True)

    if sec_code:  # Detalle de outcomes/competencias sólo cuando hay sección concreta
        det = run_query(SQL_DETALLE,{"sec":sec_code})
        st.dataframe(det, use_container_width=True)

