-- Paramètres (chemins HDFS)
%default FLIGHTS '/user/root/input/flights/flights.csv';
%default CARRIERS '/user/root/input/flights/carriers.csv';
%default AIRPORTS '/user/root/input/flights/airports.csv';
%default OUTDIR '/user/root/pigout/flights';

-- Ingestion du dataset vols (DataExpo 2009)
vols_raw = LOAD '$FLIGHTS' USING PigStorage(',') AS (
  an:int, mois:int, jour:int, jour_semaine:int,
  heure_dep:int, heure_plan_dep:int, heure_arr:int, heure_plan_arr:int,
  transporteur:chararray, num_vol:chararray, immatriculation:chararray,
  tps_total:int, tps_plan:int, tps_air:int,
  retard_arr:int, retard_dep:int, origine:chararray, destination:chararray,
  distance:int, taxi_in:int, taxi_out:int, annule:int,
  code_annulation:chararray, devie:int,
  retard_transporteur:int, retard_meteo:int, retard_nas:int,
  retard_securite:int, retard_appareil:int);

vols = FILTER vols_raw BY an IS NOT NULL AND origine IS NOT NULL AND destination IS NOT NULL;

-- Transporteurs
tc_raw = LOAD '$CARRIERS' USING PigStorage(',') AS (code:chararray, libelle:chararray);
tc_clean = FILTER tc_raw BY code IS NOT NULL AND UPPER(code) != 'CODE';
tc = FOREACH tc_clean GENERATE REPLACE(code,'"','') AS code, REPLACE(libelle,'"','') AS libelle;

-- Aéroports
apt_raw = LOAD '$AIRPORTS' USING PigStorage(',') AS (iata:chararray, aeroport:chararray, ville:chararray, etat:chararray, pays:chararray, lat:double, lon:double);
apt_f = FILTER apt_raw BY iata IS NOT NULL AND LOWER(iata) != 'iata';
apt = FOREACH apt_f GENERATE REPLACE(iata,'"','') AS iata,
                              REPLACE(aeroport,'"','') AS aeroport,
                              REPLACE(ville,'"','') AS ville,
                              REPLACE(etat,'"','') AS etat,
                              REPLACE(pays,'"','') AS pays;

-- (1) Top 20 aéroports (sortant / entrant / total)
grp_out = GROUP vols BY origine;
stat_out = FOREACH grp_out GENERATE group AS iata, COUNT(vols) AS sortants;
grp_in = GROUP vols BY destination;
stat_in = FOREACH grp_in GENERATE group AS iata, COUNT(vols) AS entrants;
fusion_vol = JOIN stat_out BY iata FULL, stat_in BY iata;
fusion_calc = FOREACH fusion_vol GENERATE
  (stat_out::iata IS NOT NULL ? stat_out::iata : stat_in::iata) AS iata,
  (stat_out::sortants IS NOT NULL ? stat_out::sortants : 0L) AS sortants,
  (stat_in::entrants IS NOT NULL ? stat_in::entrants : 0L) AS entrants,
  ((stat_out::sortants IS NOT NULL ? stat_out::sortants : 0L) + (stat_in::entrants IS NOT NULL ? stat_in::entrants : 0L)) AS total;
fusion_nom = JOIN fusion_calc BY iata LEFT, apt BY iata;
top_aero = ORDER fusion_nom BY fusion_calc::total DESC;
top20_aero = LIMIT top_aero 20;
STORE top20_aero INTO '$OUTDIR/top20_airports' USING PigStorage('\t');

-- (1b) Volumes mensuels par aéroport
grp_out_m = GROUP vols BY (an, mois, origine);
stat_out_m = FOREACH grp_out_m GENERATE group.an AS an, group.mois AS mois, group.origine AS iata, COUNT(vols) AS sortants;
grp_in_m = GROUP vols BY (an, mois, destination);
stat_in_m = FOREACH grp_in_m GENERATE group.an AS an, group.mois AS mois, group.destination AS iata, COUNT(vols) AS entrants;
fusion_m = JOIN stat_out_m BY (an, mois, iata) FULL, stat_in_m BY (an, mois, iata);
fusion_m_calc = FOREACH fusion_m GENERATE
  (stat_out_m::an IS NOT NULL ? stat_out_m::an : stat_in_m::an) AS an,
  (stat_out_m::mois IS NOT NULL ? stat_out_m::mois : stat_in_m::mois) AS mois,
  (stat_out_m::iata IS NOT NULL ? stat_out_m::iata : stat_in_m::iata) AS iata,
  (stat_out_m::sortants IS NOT NULL ? stat_out_m::sortants : 0L) AS sortants,
  (stat_in_m::entrants IS NOT NULL ? stat_in_m::entrants : 0L) AS entrants,
  ((stat_out_m::sortants IS NOT NULL ? stat_out_m::sortants : 0L) + (stat_in_m::entrants IS NOT NULL ? stat_in_m::entrants : 0L)) AS total;
STORE fusion_m_calc INTO '$OUTDIR/airports_monthly' USING PigStorage('\t');

-- (2) Popularité transporteurs (log10 volume annuel + classement moyenne)
grp_carrier_year = GROUP vols BY (an, transporteur);
carrier_year = FOREACH grp_carrier_year GENERATE group.transporteur AS carrier, group.an AS an, COUNT(vols) AS n;
carrier_year_log = FOREACH carrier_year GENERATE carrier, an, (double)(LOG((double)n)/LOG(10.0)) AS log10_n, n;
grp_carrier = GROUP carrier_year BY carrier;
carrier_stats = FOREACH grp_carrier GENERATE group AS carrier, AVG(carrier_year.n) AS moyenne_annuelle;
carrier_join = JOIN carrier_year_log BY carrier, carrier_stats BY carrier;
carrier_rank = ORDER carrier_join BY carrier_stats::moyenne_annuelle DESC, carrier_year_log::carrier ASC;
STORE carrier_rank INTO '$OUTDIR/carriers_popularity' USING PigStorage('\t');

-- (3) Retards >15 min (heure, jour, jour semaine, mois, année)
vols_delay = FOREACH vols GENERATE
  an, mois, jour, jour_semaine,
  (heure_dep IS NOT NULL ? (int)(heure_dep/100) : (heure_plan_dep IS NOT NULL ? (int)(heure_plan_dep/100) : -1)) AS heure,
  transporteur AS carrier,
  (retard_arr IS NOT NULL AND retard_arr > 15 ? 1 : 0) AS est_retard:int;

grp_h = GROUP vols_delay BY heure;
delay_h = FOREACH grp_h GENERATE group AS heure, AVG(vols_delay.est_retard) AS frac_retard, COUNT(vols_delay) AS n;
STORE delay_h INTO '$OUTDIR/delay_rate_hour' USING PigStorage('\t');

grp_d = GROUP vols_delay BY (an, mois, jour);
delay_d = FOREACH grp_d GENERATE group.an AS an, group.mois AS mois, group.jour AS jour, AVG(vols_delay.est_retard) AS frac_retard, COUNT(vols_delay) AS n;
STORE delay_d INTO '$OUTDIR/delay_rate_day' USING PigStorage('\t');

grp_dow = GROUP vols_delay BY jour_semaine;
delay_dow = FOREACH grp_dow GENERATE group AS jour_semaine, AVG(vols_delay.est_retard) AS frac_retard, COUNT(vols_delay) AS n;
STORE delay_dow INTO '$OUTDIR/delay_rate_dayofweek' USING PigStorage('\t');

grp_m = GROUP vols_delay BY (an, mois);
delay_m = FOREACH grp_m GENERATE group.an AS an, group.mois AS mois, AVG(vols_delay.est_retard) AS frac_retard, COUNT(vols_delay) AS n;
STORE delay_m INTO '$OUTDIR/delay_rate_month' USING PigStorage('\t');

grp_y = GROUP vols_delay BY an;
delay_y = FOREACH grp_y GENERATE group AS an, AVG(vols_delay.est_retard) AS frac_retard, COUNT(vols_delay) AS n;
STORE delay_y INTO '$OUTDIR/delay_rate_year' USING PigStorage('\t');

-- (4) Retards agrégés par transporteur (global + mensuel)
grp_c = GROUP vols_delay BY carrier;
delay_c = FOREACH grp_c GENERATE group AS carrier, AVG(vols_delay.est_retard) AS frac_retard, COUNT(vols_delay) AS n;
delay_c_named = JOIN delay_c BY carrier LEFT, tc BY code;
STORE delay_c_named INTO '$OUTDIR/delay_rate_carrier' USING PigStorage('\t');

grp_c_m = GROUP vols_delay BY (carrier, an, mois);
delay_c_m = FOREACH grp_c_m GENERATE group.carrier AS carrier, group.an AS an, group.mois AS mois, AVG(vols_delay.est_retard) AS frac_retard, COUNT(vols_delay) AS n;
STORE delay_c_m INTO '$OUTDIR/delay_rate_carrier_month' USING PigStorage('\t');

-- (5) Itinéraires les plus fréquentés (paire non ordonnée normalisée)
routes = FOREACH vols GENERATE
  (origine <= destination ? origine : destination) AS a1,
  (origine <= destination ? destination : origine) AS a2;
grp_route = GROUP routes BY (a1, a2);
route_freq = FOREACH grp_route GENERATE group.a1 AS a1, group.a2 AS a2, COUNT(routes) AS n;
route_freq_ord = ORDER route_freq BY n DESC;
top_routes = LIMIT route_freq_ord 50;
rt_a1 = JOIN top_routes BY a1 LEFT, apt BY iata;
rt_a2 = JOIN rt_a1 BY top_routes::a2 LEFT, apt BY iata;
routes_nom = FOREACH rt_a2 GENERATE
  rt_a1::top_routes::a1 AS a1,
  rt_a1::top_routes::a2 AS a2,
  rt_a1::top_routes::n AS n,
  rt_a1::apt::aeroport AS a1_nom,
  apt::aeroport AS a2_nom;
STORE routes_nom INTO '$OUTDIR/top_routes' USING PigStorage('\t');
