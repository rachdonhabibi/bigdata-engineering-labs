-- Paramètres (chemins HDFS)
%default FLIGHTS '/user/root/input/flights/flights.csv';
%default CARRIERS '/user/root/input/flights/carriers.csv';
%default AIRPORTS '/user/root/input/flights/airports.csv';
%default OUTDIR '/user/root/pigout/flights';

-- Charger les vols (schéma DataExpo 2009)
FL_RAW = LOAD '$FLIGHTS' USING PigStorage(',')
  AS (Year:int, Month:int, DayofMonth:int, DayOfWeek:int,
      DepTime:int, CRSDepTime:int, ArrTime:int, CRSArrTime:int,
      UniqueCarrier:chararray, FlightNum:chararray, TailNum:chararray,
      ActualElapsedTime:int, CRSElapsedTime:int, AirTime:int,
      ArrDelay:int, DepDelay:int, Origin:chararray, Dest:chararray,
      Distance:int, TaxiIn:int, TaxiOut:int, Cancelled:int,
      CancellationCode:chararray, Diverted:int,
      CarrierDelay:int, WeatherDelay:int, NASDelay:int,
      SecurityDelay:int, LateAircraftDelay:int);

-- Filtrer les lignes valides (exclut entêtes et codes vides)
FL = FILTER FL_RAW BY Year IS NOT NULL AND Origin IS NOT NULL AND Dest IS NOT NULL;

-- Charger les transporteurs (retirer l’en-tête et les guillemets)
CARR_RAW = LOAD '$CARRIERS' USING PigStorage(',')
  AS (code:chararray, descr:chararray);
CARR = FILTER CARR_RAW BY code IS NOT NULL AND UPPER(code) != 'CODE';
CARR = FOREACH CARR GENERATE REPLACE(code,'"','') AS code, REPLACE(descr,'"','') AS descr;

-- Charger les aéroports (retirer l’en-tête et les guillemets)
APT_RAW = LOAD '$AIRPORTS' USING PigStorage(',')
  AS (iata:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, lat:double, lon:double);
APT = FILTER APT_RAW BY iata IS NOT NULL AND LOWER(iata) != 'iata';
APT = FOREACH APT GENERATE
  REPLACE(iata,'"','') AS iata,
  REPLACE(airport,'"','') AS airport,
  REPLACE(city,'"','') AS city,
  REPLACE(state,'"','') AS state,
  REPLACE(country,'"','') AS country;

-- 1) Top 20 des aéroports (sortants, entrants, total)
OUT_G = GROUP FL BY Origin;
OUT_C = FOREACH OUT_G GENERATE group AS iata, COUNT(FL) AS out_cnt;

IN_G = GROUP FL BY Dest;
IN_C = FOREACH IN_G GENERATE group AS iata, COUNT(FL) AS in_cnt;

VOL = JOIN OUT_C BY iata FULL, IN_C BY iata;
VOL_ENR = FOREACH VOL GENERATE
  (OUT_C::iata IS NOT NULL ? OUT_C::iata : IN_C::iata) AS iata,
  (OUT_C::out_cnt IS NOT NULL ? OUT_C::out_cnt : 0L) AS out_cnt,
  (IN_C::in_cnt IS NOT NULL ? IN_C::in_cnt : 0L) AS in_cnt,
  ((OUT_C::out_cnt IS NOT NULL ? OUT_C::out_cnt : 0L) + (IN_C::in_cnt IS NOT NULL ? IN_C::in_cnt : 0L)) AS total_cnt;

VOL_NAMED = JOIN VOL_ENR BY iata LEFT, APT BY iata;
TOP_AIRPORTS = ORDER VOL_NAMED BY VOL_ENR::total_cnt DESC;
TOP20_AIRPORTS = LIMIT TOP_AIRPORTS 20;
STORE TOP20_AIRPORTS INTO '$OUTDIR/top20_airports' USING PigStorage('\t');

-- 1bis) Volumes par (année, mois, aéroport)
OUT_M_G = GROUP FL BY (Year, Month, Origin);
OUT_M = FOREACH OUT_M_G GENERATE group.Year AS year, group.Month AS month, group.Origin AS iata, COUNT(FL) AS out_cnt;
IN_M_G = GROUP FL BY (Year, Month, Dest);
IN_M = FOREACH IN_M_G GENERATE group.Year AS year, group.Month AS month, group.Dest AS iata, COUNT(FL) AS in_cnt;

VOL_M = JOIN OUT_M BY (year, month, iata) FULL, IN_M BY (year, month, iata);
VOL_M_ENR = FOREACH VOL_M GENERATE
  (OUT_M::year IS NOT NULL ? OUT_M::year : IN_M::year) AS year,
  (OUT_M::month IS NOT NULL ? OUT_M::month : IN_M::month) AS month,
  (OUT_M::iata IS NOT NULL ? OUT_M::iata : IN_M::iata) AS iata,
  (OUT_M::out_cnt IS NOT NULL ? OUT_M::out_cnt : 0L) AS out_cnt,
  (IN_M::in_cnt IS NOT NULL ? IN_M::in_cnt : 0L) AS in_cnt,
  ((OUT_M::out_cnt IS NOT NULL ? OUT_M::out_cnt : 0L) + (IN_M::in_cnt IS NOT NULL ? IN_M::in_cnt : 0L)) AS total_cnt;
STORE VOL_M_ENR INTO '$OUTDIR/airports_monthly' USING PigStorage('\t');

-- 2) Popularité des transporteurs (log10 du volume annuel, tri par «médiane» ~ moyenne)
FL_YC_G = GROUP FL BY (Year, UniqueCarrier);
YC_YEARLY = FOREACH FL_YC_G GENERATE group.UniqueCarrier AS carrier, group.Year AS year, COUNT(FL) AS n;
YC_YEARLY_LOG = FOREACH YC_YEARLY GENERATE carrier, year, (double)(LOG((double)n)/LOG(10.0)) AS log10_n, n;

YC_G = GROUP YC_YEARLY BY carrier;
YC_STATS = FOREACH YC_G GENERATE group AS carrier, AVG(YC_YEARLY.n) AS median_like;
YC_JOINED = JOIN YC_YEARLY_LOG BY carrier, YC_STATS BY carrier;
YC_RANKED = ORDER YC_JOINED BY YC_STATS::median_like DESC, YC_YEARLY_LOG::carrier ASC;
STORE YC_RANKED INTO '$OUTDIR/carriers_popularity' USING PigStorage('\t');

-- 3) Proportion de vols en retard (>15 min) par heure/jour/jour-semaine/mois/année
FL_DLY = FOREACH FL GENERATE
  Year, Month, DayofMonth, DayOfWeek,
  (DepTime IS NOT NULL ? (int)(DepTime/100) : (CRSDepTime IS NOT NULL ? (int)(CRSDepTime/100) : -1)) AS hour,
  UniqueCarrier AS carrier,
  (ArrDelay IS NOT NULL AND ArrDelay > 15 ? 1 : 0) AS is_delayed:int;

G_H = GROUP FL_DLY BY hour;
DELAY_RATE_H = FOREACH G_H GENERATE group AS hour, AVG(FL_DLY.is_delayed) AS frac_delayed, COUNT(FL_DLY) AS n;
STORE DELAY_RATE_H INTO '$OUTDIR/delay_rate_hour' USING PigStorage('\t');

G_D = GROUP FL_DLY BY (Year, Month, DayofMonth);
DELAY_RATE_D = FOREACH G_D GENERATE group.Year AS year, group.Month AS month, group.DayofMonth AS day, AVG(FL_DLY.is_delayed) AS frac_delayed, COUNT(FL_DLY) AS n;
STORE DELAY_RATE_D INTO '$OUTDIR/delay_rate_day' USING PigStorage('\t');

G_DOW = GROUP FL_DLY BY DayOfWeek;
DELAY_RATE_DOW = FOREACH G_DOW GENERATE group AS day_of_week, AVG(FL_DLY.is_delayed) AS frac_delayed, COUNT(FL_DLY) AS n;
STORE DELAY_RATE_DOW INTO '$OUTDIR/delay_rate_dayofweek' USING PigStorage('\t');

G_MO = GROUP FL_DLY BY (Year, Month);
DELAY_RATE_MO = FOREACH G_MO GENERATE group.Year AS year, group.Month AS month, AVG(FL_DLY.is_delayed) AS frac_delayed, COUNT(FL_DLY) AS n;
STORE DELAY_RATE_MO INTO '$OUTDIR/delay_rate_month' USING PigStorage('\t');

G_Y = GROUP FL_DLY BY Year;
DELAY_RATE_Y = FOREACH G_Y GENERATE group AS year, AVG(FL_DLY.is_delayed) AS frac_delayed, COUNT(FL_DLY) AS n;
STORE DELAY_RATE_Y INTO '$OUTDIR/delay_rate_year' USING PigStorage('\t');

-- 4) Retards par transporteur (global + par mois)
G_C = GROUP FL_DLY BY carrier;
DELAY_RATE_C = FOREACH G_C GENERATE group AS carrier, AVG(FL_DLY.is_delayed) AS frac_delayed, COUNT(FL_DLY) AS n;
DELAY_RATE_C_NAMED = JOIN DELAY_RATE_C BY carrier LEFT, CARR BY code;
STORE DELAY_RATE_C_NAMED INTO '$OUTDIR/delay_rate_carrier' USING PigStorage('\t');

G_C_MO = GROUP FL_DLY BY (carrier, Year, Month);
DELAY_RATE_C_MO = FOREACH G_C_MO GENERATE group.carrier AS carrier, group.Year AS year, group.Month AS month, AVG(FL_DLY.is_delayed) AS frac_delayed, COUNT(FL_DLY) AS n;
STORE DELAY_RATE_C_MO INTO '$OUTDIR/delay_rate_carrier_month' USING PigStorage('\t');

-- 5) Itinéraires les plus fréquentés (paire non ordonnée)
ROUTES = FOREACH FL GENERATE
  (Origin <= Dest ? Origin : Dest) AS a1,
  (Origin <= Dest ? Dest : Origin) AS a2;
G_R = GROUP ROUTES BY (a1, a2);
ROUTE_FREQ = FOREACH G_R GENERATE group.a1 AS a1, group.a2 AS a2, COUNT(ROUTES) AS n;
ROUTE_FREQ_N = ORDER ROUTE_FREQ BY n DESC;
TOP_ROUTES = LIMIT ROUTE_FREQ_N 50;

R_A1 = JOIN TOP_ROUTES BY a1 LEFT, APT BY iata;
R_A2 = JOIN R_A1 BY TOP_ROUTES::a2 LEFT, APT BY iata;
ROUTES_NAMED = FOREACH R_A2 GENERATE
  R_A1::TOP_ROUTES::a1 AS a1,
  R_A1::TOP_ROUTES::a2 AS a2,
  R_A1::TOP_ROUTES::n AS n,
  R_A1::APT::airport AS a1_name,
  APT::airport AS a2_name;
STORE ROUTES_NAMED INTO '$OUTDIR/top_routes' USING PigStorage('\t');
