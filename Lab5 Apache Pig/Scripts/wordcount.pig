
%default INPUT '/user/root/input/alice.txt';
%default OUTPUT '/user/root/pigout/WORD_COUNT';

-- Lecture brute : une seule colonne (ligne entière)
src_lignes = LOAD '$INPUT' AS (texte:chararray);

-- Normalisation simple : tout en minuscules pour éviter les doublons par casse
normalise = FOREACH src_lignes GENERATE LOWER(texte) AS texte;

-- Découpage en unités lexicales (séparation naïve sur l'espace)
jetons = FOREACH normalise GENERATE FLATTEN(TOKENIZE(texte)) AS mot:chararray;

-- Filtrage : conserver uniquement les séquences alphanumériques (\\w = lettres / chiffres / _)
mot_filtre = FILTER jetons BY mot IS NOT NULL AND SIZE(mot) > 0 AND mot MATCHES '\\w+';

-- Agrégation : regroupement par terme
grp_mot = GROUP mot_filtre BY mot;
stat_mot = FOREACH grp_mot GENERATE group AS mot, COUNT(mot_filtre) AS occurences;

-- Classement : fréquence décroissante puis ordre alphabétique pour lisibilité stable
classe = ORDER stat_mot BY occurences DESC, mot ASC;

-- Persistance du résultat tabulé (mot<TAB>compte)
STORE classe INTO '$OUTPUT' USING PigStorage('\t');
