# ==============================================================================
# BOLSA DE VALORES DE CARACAS — ACTUALIZADOR DIARIO
# GitHub Actions version | Corre 6am VET (10am UTC) lunes a viernes
# ==============================================================================

library(readr)
library(dplyr)
library(stringr)
library(tidyr)
library(purrr)
library(lubridate)
library(httr)

# ==============================================================================
# === 0. PARÁMETROS
# ==============================================================================

folder_dat          <- "data/raw"
tickers_path        <- "TKR.csv"
splits_path         <- "splits.csv"
ruta_salida         <- "data"
fecha_inicio_output <- as.Date("2024-01-01")
fecha_inicio_corte  <- as.Date("2021-12-31")

dir.create(folder_dat,  recursive = TRUE, showWarnings = FALSE)
dir.create(ruta_salida, recursive = TRUE, showWarnings = FALSE)

# ==============================================================================
# === 1. FECHA A DESCARGAR (día hábil anterior)
# ==============================================================================

es_dia_habil <- function(fecha) wday(fecha) %in% 2:6

fecha_ayer <- Sys.Date() - 1
while (!es_dia_habil(fecha_ayer)) fecha_ayer <- fecha_ayer - 1

cat("Fecha objetivo:", format(fecha_ayer, "%Y-%m-%d"), "\n")

# ==============================================================================
# === 2. DESCARGA INCREMENTAL
# ==============================================================================

archivos_existentes <- list.files(folder_dat, pattern = "^diario\\d{8}\\.dat$")
fechas_existentes   <- archivos_existentes |>
  str_remove_all("^diario|\\.dat$") |>
  ymd()
fechas_existentes <- fechas_existentes[!is.na(fechas_existentes)]

fechas_a_cubrir  <- seq(from = fecha_inicio_corte + 1, to = fecha_ayer, by = "day")
fechas_faltantes <- as.Date(setdiff(fechas_a_cubrir, fechas_existentes))
fechas_faltantes <- sort(fechas_faltantes[sapply(fechas_faltantes, es_dia_habil)])

cat("Archivos a descargar:", length(fechas_faltantes), "\n")

descargar_dat <- function(fecha) {
  url  <- paste0(
    "https://www.bolsadecaracas.com/descargar-diario-bolsa/?type=dat&fecha=",
    format(fecha, "%Y%m%d")
  )
  dest <- file.path(folder_dat, paste0("diario", format(fecha, "%Y%m%d"), ".dat"))
  tryCatch({
    r <- GET(url, timeout(60))
    if (status_code(r) == 200 && length(content(r, "raw")) > 100) {
      writeBin(content(r, "raw"), dest)
    } else {
      # Archivo vacío como placeholder: evita reintentar este día en el futuro
      file.create(dest)
      message("Sin datos (feriado/mercado cerrado): ", format(fecha, "%Y-%m-%d"))
    }
  }, error = function(e) {
    message("Error red: ", format(fecha, "%Y-%m-%d"), " — ", e$message)
  })
}

walk(fechas_faltantes, descargar_dat)

# ==============================================================================
# === 3. TICKERS Y SPLITS
# ==============================================================================

tickers <- read_csv(tickers_path, col_names = FALSE, show_col_types = FALSE) |>
  mutate(X1 = str_trim(X1)) |>
  filter(X1 != "") |>
  pull(X1)

cat("Tickers cargados:", length(tickers), "\n")

splits <- read_csv(splits_path, show_col_types = FALSE) |>
  mutate(Fecha = as.Date(Fecha))

cat("Splits cargados:", nrow(splits), "\n")

# ==============================================================================
# === 4. FUNCIÓN MAESTRA: ONE-PASS READING
#
# Un .dat es un día bursátil real si contiene líneas "^R|" (registros de
# negociación). Archivos vacíos, placeholders de feriados y fines de semana
# no tienen esas líneas y se descartan aquí — nunca llegan al panel final.
# Esto garantiza que el output solo contiene días con actividad real en el mercado.
# ==============================================================================

tiene_datos_bursatiles <- function(lineas) any(str_detect(lineas, "^R\\|"))

procesar_archivo <- function(path) {
  fecha_archivo <- str_extract(basename(path), "\\d{8}") |> ymd()

  lineas <- tryCatch(
    read_lines(path, locale = locale(encoding = "latin1")),
    error = function(e) character(0)
  )

  # Descartar: archivo vacío, placeholder de feriado, o sin negociaciones
  if (length(lineas) == 0 || !tiene_datos_bursatiles(lineas)) {
    return(list(precios = NULL, indices = NULL, resumen = NULL))
  }

  lineas_r   <- lineas[str_detect(lineas, "^R\\|")]
  lineas_p   <- lineas[str_detect(lineas, "^P\\|")]
  lineas_idx <- lineas[str_detect(lineas, "^(IG|IF|II|TC)\\|")]

  # --- Índices ---
  if (length(lineas_idx) > 0) {
    df_idx <- map_dfr(lineas_idx, function(l) {
      partes <- str_split(l, "\\|")[[1]]
      if (length(partes) < 2) return(NULL)
      valor <- if (partes[1] == "TC") {
        as.numeric(str_extract(partes[2], "\\d+(\\.\\d+)?"))
      } else {
        suppressWarnings(as.numeric(partes[3]))
      }
      tibble(Fecha = fecha_archivo, Indice = partes[1], Valor = valor)
    })
  } else {
    df_idx <- NULL
  }

  # --- Precios de cierre (líneas R) ---
  mat_r <- str_split_fixed(lineas_r, "\\|", 18)
  df_precios <- tibble(
    Fecha  = fecha_archivo,
    Ticker = mat_r[, 3],
    Precio = suppressWarnings(as.numeric(mat_r[, 5]))
  )

  # --- Operaciones (R + P) ---
  extraer_ops <- function(lineas_raw, tipo) {
    if (length(lineas_raw) == 0) return(NULL)
    mat <- str_split_fixed(lineas_raw, "\\|", 18)
    tibble(
      Fecha    = fecha_archivo,
      Tipo_Op  = tipo,
      Ticker   = mat[, 3],
      N_ops    = suppressWarnings(as.integer(mat[, 11])),
      Cantidad = suppressWarnings(as.numeric(mat[, 12])),
      Volumen  = suppressWarnings(as.numeric(mat[, 13]))
    )
  }

  df_resumen <- bind_rows(extraer_ops(lineas_r, "R"), extraer_ops(lineas_p, "P"))

  list(precios = df_precios, indices = df_idx, resumen = df_resumen)
}

# ==============================================================================
# === 5. PROCESAR Y CONSOLIDAR
# ==============================================================================

archivos <- list.files(folder_dat, pattern = "^diario\\d{8}\\.dat$", full.names = TRUE) |>
  keep(function(x) {
    f <- str_extract(basename(x), "\\d{8}") |> ymd()
    f > fecha_inicio_corte & f <= fecha_ayer
  })

cat("Archivos .dat a procesar:", length(archivos), "\n")
cat("Procesando...\n")

data_raw <- map(archivos, procesar_archivo)

cat("Consolidando...\n")
df_precios_raw <- map_dfr(data_raw, "precios") |>
  filter(Ticker %in% tickers, !is.na(Precio))

df_indices_raw <- map_dfr(data_raw, "indices")

df_ops_raw <- map_dfr(data_raw, "resumen") |>
  filter(Ticker %in% tickers)

# ==============================================================================
# === 6. VALIDACIÓN TEMPRANA
# Aborta antes de tocar cualquier CSV si los datos crudos están vacíos
# ==============================================================================

if (nrow(df_precios_raw) == 0) {
  stop("CRITICO: df_precios_raw vacio. ",
       "Posible cambio en formato .dat o fallo masivo de descarga. ",
       "CSVs anteriores NO sobreescritos.")
}
if (nrow(df_indices_raw) == 0) {
  stop("CRITICO: df_indices_raw vacio (sin TC ni IBC). ",
       "CSVs anteriores NO sobreescritos.")
}

cat("Datos crudos OK — filas precios:", nrow(df_precios_raw),
    "| filas indices:", nrow(df_indices_raw), "\n")

# ==============================================================================
# === 7. TRANSFORMACIONES
# ==============================================================================

# 7.1 Índices wide + corrección de escala IBC (la BVC cambió escala en jul-2025)
df_indices <- df_indices_raw |>
  pivot_wider(names_from = Indice, values_from = Valor) |>
  mutate(across(
    any_of(c("IG", "IF", "II")),
    ~ if_else(Fecha < as.Date("2025-07-28"), .x / 1000, .x)
  )) |>
  rename(IBC = any_of("IG"))

# 7.2 Panel de precios — SOLO días bursátiles reales
# left_join (no full_join + complete): no se fabrican filas de fines de semana
# ni feriados. El forward-fill aplica solo entre días con datos de mercado.
df_panel <- df_precios_raw |>
  pivot_wider(names_from = Ticker, values_from = Precio, values_fn = mean) |>
  left_join(df_indices, by = "Fecha") |>
  arrange(Fecha)

tickers_presentes <- intersect(tickers, names(df_panel))

# Garantizar existencia de columnas opcionales de índices
if (!"IF" %in% names(df_panel)) df_panel$IF <- NA_real_
if (!"II" %in% names(df_panel)) df_panel$II <- NA_real_

indices_orden <- intersect(c("TC", "IBC", "IF", "II"), names(df_panel))

df_panel <- df_panel |>
  select(all_of(c("Fecha", indices_orden, tickers_presentes))) |>
  # Forward-fill: ticker sin precio ese día usa el último precio negociado
  fill(all_of(tickers_presentes), .direction = "down")

# 7.3 Splits de precio desde splits.csv
# Cada fila ajusta precios históricos ANTES de la fecha del evento corporativo
df_precios_bs <- df_panel

for (i in seq_len(nrow(splits))) {
  tkr    <- splits$Ticker[i]
  fecha  <- splits$Fecha[i]
  factor <- splits$Factor[i]
  tipo   <- splits$Tipo[i]
  if (!tkr %in% names(df_precios_bs)) next
  col <- sym(tkr)
  df_precios_bs <- df_precios_bs |>
    mutate(!!col := case_when(
      tipo == "precio_mult" & Fecha < fecha ~ !!col * factor,
      tipo == "precio_div"  & Fecha < fecha ~ !!col / factor,
      TRUE                                  ~ !!col
    ))
}

# 7.4 Precios en USD
# TC no se convierte: es la tasa de cambio (Bs/USD), no un precio en Bs
# IBC, IF, II tampoco: son índices, no precios en Bs
cols_no_convertir   <- c("Fecha", "TC", "IBC", "IF", "II")
tickers_a_convertir <- setdiff(names(df_precios_bs), cols_no_convertir)

df_precios_usd <- df_precios_bs |>
  mutate(across(
    all_of(tickers_a_convertir),
    ~ if_else(!is.na(TC) & TC > 0, round(.x / TC, 2), NA_real_)
  ))

# 7.5 Redondeo consistente
#   Bs:      2 decimales (céntimos de bolívar)
#   USD:     4 decimales (captura fracciones de centavo para acciones baratas)
#   TC:      4 decimales
#   Índices: 2 decimales
df_precios_bs <- df_precios_bs |>
  mutate(
    TC  = round(TC,  4),
    IBC = round(IBC, 2),
    IF  = round(IF,  2),
    II  = round(II,  2),
    across(all_of(tickers_presentes), ~ round(.x, 2))
  )

df_precios_usd <- df_precios_usd |>
  mutate(
    TC  = round(TC,  4),
    IBC = round(IBC, 2),
    IF  = round(IF,  2),
    II  = round(II,  2),
    across(all_of(tickers_a_convertir), ~ round(.x, 2))
  )

# ==============================================================================
# === 8. OPERACIONES
# ==============================================================================

df_ops <- df_ops_raw |>
  group_by(Fecha, Ticker) |>
  summarise(
    Ops_Total     = sum(N_ops,    na.rm = TRUE),
    Cant_Total    = sum(Cantidad, na.rm = TRUE),
    Volumen_Total = sum(Volumen,  na.rm = TRUE),
    .groups = "drop"
  )

# Splits en cantidades (inverso al precio: precio ÷ N → cantidad × N)
for (i in seq_len(nrow(splits))) {
  tkr   <- splits$Ticker[i]
  fecha <- splits$Fecha[i]
  fac   <- splits$Factor[i]
  tipo  <- splits$Tipo[i]
  df_ops <- df_ops |>
    mutate(Cant_Total = case_when(
      Ticker == tkr & tipo == "precio_mult" & Fecha < fecha ~ Cant_Total / fac,
      Ticker == tkr & tipo == "precio_div"  & Fecha < fecha ~ Cant_Total * fac,
      TRUE ~ Cant_Total
    ))
}

# Volumen USD + redondeo
# Días sin operaciones quedan en 0 (no NA) — sumas de columna funcionan directamente
df_ops <- df_ops |>
  left_join(df_panel |> select(Fecha, TC), by = "Fecha") |>
  mutate(
    Volumen_USD   = if_else(!is.na(TC) & TC > 0, round(Volumen_Total / TC, 2), 0),
    Volumen_Total = round(Volumen_Total, 2),
    Cant_Total    = round(Cant_Total,    2)
  ) |>
  select(-TC)

# Wide: una columna por ticker, 0 donde no hubo operaciones ese día
crear_wide <- function(data, col_valor) {
  data |>
    select(Fecha, Ticker, all_of(col_valor)) |>
    pivot_wider(
      names_from  = Ticker,
      values_from = all_of(col_valor),
      values_fill = 0
    ) |>
    arrange(Fecha)
}

ops_por_dia  <- crear_wide(df_ops, "Ops_Total")
cant_por_dia <- crear_wide(df_ops, "Cant_Total")
volumen_bs   <- crear_wide(df_ops, "Volumen_Total")
volumen_usd  <- crear_wide(df_ops, "Volumen_USD")

# ==============================================================================
# === 9. VALIDACIÓN FINAL
# Todos los outputs se validan antes de escribir el primer archivo
# ==============================================================================

validar_df <- function(df, nombre) {
  if (nrow(df) == 0)           stop("CRITICO: '", nombre, "' vacio — abortando sin sobreescribir.")
  if (!"Fecha" %in% names(df)) stop("CRITICO: '", nombre, "' sin columna Fecha.")
  cat("  OK:", nombre, "-", nrow(df), "filas,", ncol(df) - 1, "columnas\n")
}

cat("Validando outputs:\n")
validar_df(df_precios_bs,  "precios_bs")
validar_df(df_precios_usd, "precios_usd")
validar_df(ops_por_dia,    "operaciones")
validar_df(cant_por_dia,   "cantidades")
validar_df(volumen_bs,     "volumen_bs")
validar_df(volumen_usd,    "volumen_usd")

# ==============================================================================
# === 10. EXPORTAR CSVs (desde fecha_inicio_output)
# ==============================================================================

filtrar <- function(df) filter(df, Fecha >= fecha_inicio_output)

write_csv(filtrar(df_precios_bs),  file.path(ruta_salida, "precios_bs.csv"))
write_csv(filtrar(df_precios_usd), file.path(ruta_salida, "precios_usd.csv"))
write_csv(filtrar(ops_por_dia),    file.path(ruta_salida, "operaciones.csv"))
write_csv(filtrar(cant_por_dia),   file.path(ruta_salida, "cantidades.csv"))
write_csv(filtrar(volumen_bs),     file.path(ruta_salida, "volumen_bs.csv"))
write_csv(filtrar(volumen_usd),    file.path(ruta_salida, "volumen_usd.csv"))

# ==============================================================================
# === 11. METADATA
# Archivo liviano para verificar frescura sin descargar los CSVs completos
# ==============================================================================

precios_filtrados <- filtrar(df_precios_bs)

metadata <- tibble(
  campo = c(
    "ultima_actualizacion",
    "fecha_mas_reciente",
    "fecha_mas_antigua",
    "dias_bursatiles_publicados",
    "tickers_activos",
    "generado_utc"
  ),
  valor = c(
    format(fecha_ayer,                         "%Y-%m-%d"),
    format(max(precios_filtrados$Fecha),        "%Y-%m-%d"),
    format(min(precios_filtrados$Fecha),        "%Y-%m-%d"),
    as.character(nrow(precios_filtrados)),
    as.character(length(tickers_presentes)),
    format(Sys.time(),                          "%Y-%m-%d %H:%M:%S UTC")
  )
)

write_csv(metadata, file.path(ruta_salida, "metadata.csv"))

cat("Proceso finalizado:", format(Sys.time(), "%Y-%m-%d %H:%M:%S UTC"), "\n")
