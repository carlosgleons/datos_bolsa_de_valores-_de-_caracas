# 📈 Datos Históricos — Bolsa de Valores de Caracas (BVC)

Base de datos pública y de actualización diaria automática del mercado de valores venezolano.  
Construida porque la BVC no ofrece un histórico consolidado accesible al público.

**Actualización:** Lunes a viernes, 6:00 AM (hora Venezuela)  
**Cobertura:** Desde el 1 de enero de 2024 en adelante
**Dashboard:** (https://carlosgleons.github.io/datos_bolsa_de_valores-_de-_caracas/)

**Fuente:** [bolsadecaracas.com](https://www.bolsadecaracas.com)

---

## 📂 Archivos disponibles

Todos los archivos están en la carpeta `/data/`. Puedes descargarlos directamente o consumirlos con código.

| Archivo | Contenido |
|---|---|
| [`precios_bs.csv`](data/precios_bs.csv) | Precios de cierre en Bolívares + índices (IBC, TC) |
| [`precios_usd.csv`](data/precios_usd.csv) | Precios de cierre convertidos a USD |
| [`operaciones.csv`](data/operaciones.csv) | Número de operaciones por ticker por día |
| [`cantidades.csv`](data/cantidades.csv) | Acciones negociadas por ticker por día |
| [`volumen_bs.csv`](data/volumen_bs.csv) | Volumen en Bolívares por ticker por día |
| [`volumen_usd.csv`](data/volumen_usd.csv) | Volumen en USD por ticker por día |

---

## 🔗 Acceso directo (URLs para scripts)

```
https://raw.githubusercontent.com/carlosgleons/datos_bolsa_de_valores-_de-_caracas/main/data/precios_bs.csv
https://raw.githubusercontent.com/carlosgleons/datos_bolsa_de_valores-_de-_caracas/main/data/precios_usd.csv
https://raw.githubusercontent.com/carlosgleons/datos_bolsa_de_valores-_de-_caracas/main/data/operaciones.csv
https://raw.githubusercontent.com/carlosgleons/datos_bolsa_de_valores-_de-_caracas/main/data/cantidades.csv
https://raw.githubusercontent.com/carlosgleons/datos_bolsa_de_valores-_de-_caracas/main/data/volumen_bs.csv
https://raw.githubusercontent.com/carlosgleons/datos_bolsa_de_valores-_de-_caracas/main/data/volumen_usd.csv
```

---

## 💻 Cómo usar los datos

### Excel / Google Sheets
Descarga el CSV directamente desde los links de arriba, o en Excel usa:  
`Datos → Obtener datos externos → Desde web` y pega la URL.

---

## 📊 Estructura de los datos

### Precios (precios_bs.csv y precios_usd.csv)

Formato **wide**: cada columna es un ticker, cada fila es un día.

```
Fecha       TC     IBC      ABC.A   BNC    BPV   ...
2024-01-02  36.5   1234.5   3.20    18.50  0.31  ...
2024-01-03  36.6   1238.1   3.20    18.50  0.31  ...
```

- `TC`: Tasa de cambio BCV (Bs/USD)
- `IBC`: Índice Bursátil de Capitalización
- `IF`: Índice Financiero
- `II`: Índice Industrial
- Los precios se llevan hacia adelante (**forward fill**) los días sin negociación

### Tickers incluidos

`ABC.A` `ALZ.B` `ARC.A` `ARC.B` `BNC` `BPV` `BVCC` `BVL` `CCR` `CGQ` `CIE` `CRM.A`  
`DOM` `EFE` `ENV` `FVI.A` `FVI.B` `FFV.B` `FNC` `GMC.B` `GZL` `GZL.A` `GZL.B`  
`ICP.B` `IMP.A` `IMP.B` `IVC.A` `IVC.B` `MPA` `MTC.B` `MVZ.A` `MVZ.B`  
`PER` `PGR` `PIV.A` `PIV.B` `PTN` `RFM` `RST` `RST.B` `SVS` `TDV.D` `TPG` `VNA.B` `BOU`

---

## ⚙️ Ajustes corporativos aplicados

| Acción | Evento | Ajuste |
|---|---|---|
| BNC | Split 1:500 (16 enero 2025) | Precio histórico ×500, cantidad ÷500 |
| BPV | Split ×2 (11 abril 2024) | Precio ÷2 antes de esa fecha |
| BPV | Split ×3.327 (7 mayo 2024) | Precio ÷3.327 antes de esa fecha |

---

## 📝 Notas

- Los feriados venezolanos no se filtran explícitamente; los días sin datos de la BVC simplemente no generan archivo, y el precio del día anterior se mantiene (forward fill)
- El histórico de archivos `.dat` crudos **no** se sube al repositorio (están en `.gitignore`) para mantener el repo liviano
- Proyecto open source bajo licencia MIT

**Autor:** Carlos G. León | [LinkedIn](https://linkedin.com/in/carlosgleons)
