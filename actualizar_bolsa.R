# ==============================================================================
# BOLSA DE VALORES DE CARACAS - ACTUALIZADOR DIARIO
# GitHub Actions version — sin dependencias de rutas locales
# Corre a las 6am VET (10am UTC), descarga el día hábil anterior
# ==============================================================================

library(readr)
library(dplyr)
library(stringr)
library(tidyr)
library(purrr)
library(lubridate)
library(httr)
library(writexl)

# ==============================================================================
# === 0. PARÁMETROS
# ==============================================================================

# Rutas relativas al repositorio (funcionan en GitHub Actions y localmente)
folder_dat    <- "data/raw"          # Archivos .dat descargados
tickers_path  <- "TKR.csv"           # Lista de tickers
ruta_salida   <- "data"              # CSVs de output

dir.create(folder_dat, recursive = TRUE, showWarnings = FALSE)
dir.create(ruta_salida, recursive = TRUE, showWarnings = FALSE)

fecha_inicio_output <- as.Date("2024-01-01")   # Solo publicamos desde aquí
fecha_inicio_corte  <- as.Date("2021-12-31")   # Desde aquí se procesan .dat

# ==============================================================================
# === 1. CALCULAR FECHA A DESCARGAR (día hábil anterior)
# ==============================================================================

es_dia_habil <- function(fecha) {
  wday(fecha) %in% 2:6  # Lunes a Viernes (sin feriados exactos, BVC los marca como sin datos)
}

fecha_ayer <- Sys.Date() - 1
# Retroceder hasta encontrar un día hábil (cubre fines de semana y lunes)
while (!es_dia_habil(fecha_ayer)) {
  fecha_ayer <- fecha_ayer - 1
}

cat("📅 Fecha objetivo a descargar:", format(fecha_ayer, "%Y-%m-%d"), "\n")

# ==============================================================================
# === 2. DESCARGA — Solo archivos faltantes (lógica incremental)
# ==============================================================================

# Detectar .dat ya presentes en el repo
archivos_existentes <- list.files(folder_dat, pattern = "^diario\\d{8}\\.dat$")
fechas_existentes   <- archivos_existentes |>
  str_remove_all("^diario|\\.dat$") |>
  ymd()
fechas_existentes   <- fechas_existentes[!is.na(fechas_existentes)]

# Rango completo que queremos tener (desde corte hasta ayer)
fechas_a_cubrir  <- seq(from = fecha_inicio_corte + 1, to = fecha_ayer, by = "day")
fechas_faltantes <- as.Date(setdiff(fechas_a_cubrir, fechas_existentes))
fechas_faltantes <- sort(fechas_faltantes[sapply(fechas_faltantes, es_dia_habil)])

if (length(fechas_faltantes) == 0) {
  cat("✅ No hay archivos faltantes.\n")
} else {
  cat("⬇️  Descargando", length(fechas_faltantes), "archivo(s) faltante(s)...\n")
}

descargar_dat <- function(fecha) {
  url    <- paste0("https://www.bolsadecaracas.com/descargar-diario-bolsa/?type=dat&fecha=",
                   format(fecha, "%Y%m%d"))
  dest   <- file.path(folder_dat, paste0("diario", format(fecha, "%Y%m%d"), ".dat"))
  tryCatch({
    r <- GET(url, timeout(30))
    if (status_code(r) == 200 && length(content(r, "raw")) > 100) {
      writeBin(content(r, "raw"), dest)
      message("  ✅ ", basename(dest))
    } else {
      message("  ⚠️  Sin datos (feriado o mercado cerrado): ", format(fecha, "%Y-%m-%d"))
    }
  }, error = function(e) message("  ❌ Error red: ", format(fecha, "%Y-%m-%d")))
}

walk(fechas_faltantes, descargar_dat)

# ==============================================================================
# === 3. TICKERS VÁLIDOS
# ==============================================================================

tickers <- read_csv(tickers_path, col_names = FALSE, show_col_types = FALSE) |>
  mutate(X1 = str_trim(X1)) |>
  filter(X1 != "") |>
  pull(X1)

cat("📋 Tickers cargados:", length(tickers), "\n")

# ==============================================================================
# === 4. IDENTIFICAR ARCHIVOS A PROCESAR (desde corte hasta ayer)
# ==============================================================================

archivos <- list.files(folder_dat, pattern = "^diario\\d{8}\\.dat$", full.names = TRUE) |>
  keep(function(x) {
    f <- str_extract(basename(x), "\\d{8}") |> ymd()
    f > fecha_inicio_corte & f <= fecha_ayer
  })

cat("📂 Archivos .dat a procesar:", length(archivos), "\n")

# ==============================================================================
# === 5. FUNCIÓN MAESTRA: ONE PASS READING
# ==============================================================================

procesar_archivo <- function(path) {
  fecha_archivo <- str_extract(basename(path), "\\d{8}") |> ymd()
  lineas        <- tryCatch(
    read_lines(path, locale = locale(encoding = "latin1")),
    error = function(e) character(0)
  )
  if (length(lineas) == 0) return(list(precios = NULL, indices = NULL, resumen = NULL))

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
  if (length(lineas_r) > 0) {
    mat_r     <- str_split_fixed(lineas_r, "\\|", 18)
    df_precios <- tibble(
      Fecha  = fecha_archivo,
      Ticker = mat_r[, 3],
      Precio = suppressWarnings(as.numeric(mat_r[, 5]))
    )
  } else {
    df_precios <- NULL
  }

  # --- Operaciones (R + P) ---
  extraer_ops <- function(lineas_raw, tipo) {
    if (length(lineas_raw) == 0) return(NULL)
    mat <- str_split_fixed(lineas_raw, "\\|", 18)
    tibble(
      Fecha     = fecha_archivo,
      Tipo_Op   = tipo,
      Ticker    = mat[, 3],
      N_ops     = suppressWarnings(as.integer(mat[, 11])),
      Cantidad  = suppressWarnings(as.numeric(mat[, 12])),
      Volumen   = suppressWarnings(as.numeric(mat[, 13]))
    )
  }

  df_resumen <- bind_rows(extraer_ops(lineas_r, "R"), extraer_ops(lineas_p, "P"))

  list(precios = df_precios, indices = df_idx, resumen = df_resumen)
}

# ==============================================================================
# === 6. PROCESAR TODOS LOS ARCHIVOS
# ==============================================================================

cat("🚀 Procesando archivos...\n")
data_raw <- map(archivos, procesar_archivo)

cat("📦 Consolidando...\n")
df_precios_raw <- map_dfr(data_raw, "precios") |>
  filter(Ticker %in% tickers, !is.na(Precio))

df_indices_raw <- map_dfr(data_raw, "indices")

df_ops_raw <- map_dfr(data_raw, "resumen") |>
  filter(Ticker %in% tickers)

# ==============================================================================
# === 7. TRANSFORMACIONES
# ==============================================================================

# --- 7.1 Índices wide + ajuste escala IBC pre-julio 2025 ---
df_indices <- df_indices_raw |>
  pivot_wider(names_from = Indice, values_from = Valor) |>
  mutate(across(any_of(c("IG", "IF", "II")),
                ~ if_else(Fecha < as.Date("2025-07-28"), .x / 1000, .x))) |>
  rename(IBC = any_of("IG"))

# --- 7.2 Panel de precios wide ---
df_panel_pre <- df_precios_raw |>
  pivot_wider(names_from = Ticker, values_from = Precio, values_fn = mean) |>
  full_join(df_indices, by = "Fecha") |>
  arrange(Fecha)

fecha_min <- min(df_panel_pre$Fecha, na.rm = TRUE)
fecha_max <- max(df_panel_pre$Fecha, na.rm = TRUE)

df_panel <- df_panel_pre |>
  complete(Fecha = seq.Date(fecha_min, fecha_max, by = "day"))

tickers_presentes <- intersect(tickers, names(df_panel))
df_panel <- df_panel |> fill(all_of(tickers_presentes), .direction = "down")

if (!"IF" %in% names(df_panel)) df_panel$IF <- NA_real_
if (!"II" %in% names(df_panel)) df_panel$II <- NA_real_

indices_orden <- intersect(c("TC", "IBC", "IF", "II"), names(df_panel))
df_panel <- df_panel |>
  select(all_of(c("Fecha", indices_orden, tickers_presentes))) |>
  filter(!(is.na(TC) & is.na(IBC) & is.na(IF) & is.na(II)))

# --- 7.3 Ajustes por splits ---
df_precios_bs <- df_panel

# BNC: split 1:500 antes del 16/01/2025
if ("BNC" %in% names(df_precios_bs)) {
  df_precios_bs <- df_precios_bs |>
    mutate(BNC = if_else(Fecha < as.Date("2025-01-16"), BNC * 500, BNC))
}

# BPV: dos splits en 2024
if ("BPV" %in% names(df_precios_bs)) {
  F1 <- 2.000; F2 <- 3.327
  df_precios_bs <- df_precios_bs |>
    mutate(BPV = case_when(
      Fecha < as.Date("2024-04-11")                              ~ BPV / (F1 * F2),
      Fecha >= as.Date("2024-04-11") & Fecha < as.Date("2024-05-07") ~ BPV / F2,
      TRUE                                                        ~ BPV
    ))
}

# --- 7.4 Precios en USD ---
tickers_precio <- setdiff(names(df_precios_bs), c("Fecha", "TC", "IBC", "IF", "II"))
df_precios_usd <- df_precios_bs |>
  mutate(across(all_of(tickers_precio),
                ~ if_else(!is.na(TC) & TC > 0, .x / TC, NA_real_)))

# ==============================================================================
# === 8. OPERACIONES (CONSOLIDADO + SPLITS EN CANTIDADES)
# ==============================================================================

df_ops <- df_ops_raw |>
  group_by(Fecha, Ticker) |>
  summarise(
    Ops_Total     = sum(N_ops, na.rm = TRUE),
    Cant_Total    = sum(Cantidad, na.rm = TRUE),
    Volumen_Total = sum(Volumen, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    Cant_Total = if_else(Ticker == "BNC" & Fecha < as.Date("2025-01-16"),
                         Cant_Total / 500, Cant_Total),
    Cant_Total = case_when(
      Ticker == "BPV" & Fecha < as.Date("2024-04-11") ~ Cant_Total * (2.000 * 3.327),
      Ticker == "BPV" & Fecha >= as.Date("2024-04-11") & Fecha < as.Date("2024-05-07") ~ Cant_Total * 3.327,
      TRUE ~ Cant_Total
    )
  )

crear_wide <- function(data, col_valor) {
  data |>
    select(Fecha, Ticker, all_of(col_valor)) |>
    pivot_wider(names_from = Ticker, values_from = all_of(col_valor), values_fill = 0) |>
    arrange(Fecha)
}

ops_por_dia     <- crear_wide(df_ops, "Ops_Total")
cant_por_dia    <- crear_wide(df_ops, "Cant_Total")
volumen_por_dia <- crear_wide(df_ops, "Volumen_Total")

volumen_usd <- df_ops |>
  left_join(df_panel |> select(Fecha, TC), by = "Fecha") |>
  mutate(Volumen_USD = if_else(!is.na(TC) & TC > 0, Volumen_Total / TC, 0)) |>
  crear_wide("Volumen_USD")

# ==============================================================================
# === 9. FILTRAR DESDE 2024-01-01 Y EXPORTAR CSVs
# ==============================================================================

filtrar <- function(df) filter(df, Fecha >= fecha_inicio_output)

write_csv(filtrar(df_precios_bs),  file.path(ruta_salida, "precios_bs.csv"))
write_csv(filtrar(df_precios_usd), file.path(ruta_salida, "precios_usd.csv"))
write_csv(filtrar(ops_por_dia),    file.path(ruta_salida, "operaciones.csv"))
write_csv(filtrar(cant_por_dia),   file.path(ruta_salida, "cantidades.csv"))
write_csv(filtrar(volumen_por_dia),file.path(ruta_salida, "volumen_bs.csv"))
write_csv(filtrar(volumen_usd),    file.path(ruta_salida, "volumen_usd.csv"))

cat("✅ CSVs exportados en /data/:\n")
cat("   - precios_bs.csv\n   - precios_usd.csv\n   - operaciones.csv\n")
cat("   - cantidades.csv\n   - volumen_bs.csv\n   - volumen_usd.csv\n")
cat("🏁 Proceso finalizado:", format(Sys.time(), "%Y-%m-%d %H:%M:%S UTC"), "\n")
