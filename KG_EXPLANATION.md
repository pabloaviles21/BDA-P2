# Knowledge Graph Exploitation Zone

## Objetivo

La Exploitation Zone de la P2 se ha modelado como un Knowledge Graph en RDF/RDFS para integrar semanticamente las tres fuentes ya limpiadas en la Trusted Zone:

- accidentes de trafico en NYC;
- meteorologia diaria de NYC;
- actividad semanal de Uber en NYC.

El objetivo no es inventar relaciones que no existen en los datos, sino crear una representacion comun que pueda ser explotada despues por pipelines de analisis con SPARQL y con embeddings de grafo. Por eso el grafo usa como eje principal el dia de 2016, que es la unica dimension compartida por las tres fuentes. Los accidentes aportan ademas dimensiones semanticas utiles: borough, tipo de vehiculo y factor contribuyente.

## Entradas y salidas

Entrada principal:

- `trusted_zone.db`
  - `accidents_data`
  - `weather_data`
  - `uber_data`

Salidas:

- `exploitation_zone.db`: materializacion relacional del KG y de las tablas agregadas.
- `exploitation_zone.ttl`: serializacion RDF/RDFS en Turtle.

El pipeline que genera estas salidas esta en:

- `scripts/exploitation_zone.py`

## Criterio de integracion

Las fuentes originales no estan perfectamente relacionadas entre si:

- `weather_data` esta a nivel diario.
- `uber_data` esta a nivel semanal y ciudad completa.
- `accidents_data` esta a nivel de accidente individual, con fecha, borough, vehiculos y factores.

Por tanto, la integracion se realiza asi:

- Weather se conecta a cada dia mediante una observacion meteorologica diaria.
- Uber se reparte de semana a dias mediante pesos por dia de la semana y se conecta al dia como observacion de movilidad.
- Accidentes se agregan por dia y borough, y se conectan al dia como observaciones de seguridad.
- Los tipos de vehiculo y factores contribuyentes se agregan por dia y se modelan como nodos semanticos reutilizables.

Esta decision conserva la trazabilidad de las fuentes y evita joins artificiales. Por ejemplo, no se une Uber con borough porque el dataset de Uber no contiene borough.

## Modelo RDFS

El KG define las siguientes clases:

| Clase | Significado |
| --- | --- |
| `ex:Day` | Dia natural de 2016 usado como eje temporal comun. |
| `ex:WeatherObservation` | Observacion meteorologica diaria. |
| `ex:MobilityObservation` | Actividad diaria estimada de Uber. |
| `ex:SafetyObservation` | Resumen diario de accidentes por borough. |
| `ex:Borough` | Borough de NYC. Incluye `UNKNOWN` cuando el dato falta. |
| `ex:VehicleType` | Tipo de vehiculo involucrado en accidentes. |
| `ex:ContributingFactor` | Factor contribuyente del accidente. |
| `ex:FactorCount` | Conteo diario de apariciones de un factor. |
| `ex:VehicleTypeCount` | Conteo diario de apariciones de un tipo de vehiculo. |

## Relaciones principales

| Predicado | Dominio aproximado | Rango aproximado | Uso |
| --- | --- | --- | --- |
| `ex:hasWeatherObservation` | `Day` | `WeatherObservation` | Conecta un dia con su clima. |
| `ex:hasMobilityObservation` | `Day` | `MobilityObservation` | Conecta un dia con la estimacion de Uber. |
| `ex:hasSafetyObservation` | `Day` | `SafetyObservation` | Conecta un dia con resumenes de accidentes por borough. |
| `ex:observedInBorough` | `SafetyObservation` | `Borough` | Indica el borough de una observacion de seguridad. |
| `ex:hasFactorCount` | `Day` | `FactorCount` | Conecta un dia con el conteo de un factor. |
| `ex:aboutFactor` | `FactorCount` | `ContributingFactor` | Indica que factor se esta contando. |
| `ex:hasVehicleTypeCount` | `Day` | `VehicleTypeCount` | Conecta un dia con el conteo de un tipo de vehiculo. |
| `ex:aboutVehicleType` | `VehicleTypeCount` | `VehicleType` | Indica que tipo de vehiculo se esta contando. |

## Propiedades literales

Los nodos contienen propiedades literales para que los pipelines analiticos puedan generar features directamente desde el grafo.

Ejemplos:

- `ex:eventDate`
- `ex:isWeekend`
- `ex:averageTemperature`
- `ex:precipitation`
- `ex:snowFall`
- `ex:estimatedDispatchedTrips`
- `ex:estimatedUniqueVehicles`
- `ex:activeBases`
- `ex:collisions`
- `ex:personsInjured`
- `ex:personsKilled`
- `ex:occurrences`

## Tablas generadas en DuckDB

`exploitation_zone.db` incluye tablas intermedias y tablas especificas del KG:

| Tabla | Descripcion |
| --- | --- |
| `weather_daily` | Clima diario normalizado. |
| `uber_period_days` | Expansion de registros semanales de Uber a dias. |
| `uber_daily` | Actividad diaria estimada de Uber. |
| `accidents_2016` | Accidentes filtrados a 2016. |
| `accidents_daily` | Accidentes agregados por dia. |
| `borough_daily_safety` | Accidentes agregados por dia y borough. |
| `factor_daily` | Apariciones diarias de factores contribuyentes. |
| `vehicle_type_daily` | Apariciones diarias de tipos de vehiculo. |
| `calendar_days` | Union de dias disponibles en las fuentes. |
| `analysis_ready_nyc_2016` | Vista tabular para modelos tradicionales o comparativas. |
| `kg_nodes` | Nodos del KG: identificador, tipo y etiqueta. |
| `kg_edges` | Relaciones entre nodos. |
| `kg_literals` | Propiedades literales de los nodos. |
| `kg_metadata` | Metricas de cobertura del KG generado. |

## Cobertura observada con los datos disponibles

Al probar el pipeline con la Trusted Zone de la P1 se obtuvo:

| Metrica | Valor |
| --- | ---: |
| Dias de calendario | 366 |
| Dias con weather | 325 |
| Dias con Uber | 359 |
| Dias con accidentes | 366 |
| Dias con las tres fuentes | 319 |
| Nodos KG | 13.904 |
| Relaciones KG | 26.254 |
| Literales KG | 20.936 |

Esto confirma que hay suficiente estructura para una P2 razonable, aunque la integracion queda limitada por la granularidad de las fuentes. La parte mas fuerte del KG esta en accidentes, porque es donde existen entidades semanticas mas ricas: boroughs, tipos de vehiculo y factores.

## Ejemplo conceptual

Un dia del grafo puede tener esta forma:

```text
ex:day/2016-03-15
    a ex:Day
    ex:eventDate "2016-03-15"^^xsd:date
    ex:isWeekend "false"^^xsd:boolean
    ex:hasWeatherObservation ex:weather-observation/2016-03-15
    ex:hasMobilityObservation ex:mobility-observation/2016-03-15
    ex:hasSafetyObservation ex:safety-observation/2016-03-15/brooklyn
    ex:hasFactorCount ex:factor-count/2016-03-15/driver-inattention-distraction
    ex:hasVehicleTypeCount ex:vehicle-count/2016-03-15/passenger-vehicle
```

Esta estructura permite consultar patrones como:

- dias con alta precipitacion y muchos accidentes;
- boroughs con mas colisiones en fines de semana;
- factores contribuyentes asociados a dias con alto trafico estimado;
- tipos de vehiculo mas frecuentes bajo condiciones meteorologicas adversas.

## Uso posterior en analysis pipelines

El KG esta preparado para dos tipos de analisis pedidos en la P2:

1. Pipeline basado en SPARQL o pattern matching:
   - consultas sobre dias, boroughs, factores, vehiculos y clima;
   - ranking de patrones de riesgo;
   - comparacion entre dias con y sin condiciones meteorologicas adversas.

2. Pipeline basado en embeddings:
   - transformar `kg_nodes` y `kg_edges` en un grafo para generar embeddings;
   - usar los embeddings de `Day`, `Borough`, `FactorCount` o `VehicleTypeCount` como features;
   - entrenar un modelo de clustering, regresion o clasificacion sobre los dias.

## Limitaciones

- La fuente de Uber no tiene localizacion por borough, por lo que solo se integra a nivel ciudad-dia.
- Weather tambien se integra a nivel ciudad-dia, no por borough.
- La tabla de weather tiene menos dias que el calendario completo de 2016.
- Algunas colisiones no tienen borough; se conservan como `UNKNOWN` para no perder datos.
- La expansion semanal de Uber a dias es una estimacion. Se usan pesos por dia de la semana para aproximar la distribucion diaria, manteniendo los totales semanales.

Estas limitaciones estan reflejadas en el modelo para que los analisis posteriores sean interpretables y no parezcan mas precisos de lo que permiten los datos.
