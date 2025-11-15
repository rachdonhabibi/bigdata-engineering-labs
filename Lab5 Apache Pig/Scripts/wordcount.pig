%default INPUT '/user/root/input/alice.txt';
%default OUTPUT '/user/root/pigout/WORD_COUNT';

-- Charger le fichier ligne par ligne (alice.txt n'a pas de séparateur, une seule colonne)
lines = LOAD '$INPUT' AS (line:chararray);

-- Mettre en minuscules
lowered = FOREACH lines GENERATE LOWER(line) AS line;

-- Découper en mots (tokenisation) sur les espaces
words = FOREACH lowered GENERATE FLATTEN(TOKENIZE(line)) AS word:chararray;

-- Garder uniquement les mots non vides (alphanumériques et "_")
clean = FILTER words BY word IS NOT NULL AND SIZE(word) > 0 AND word MATCHES '\\w+';

-- Grouper par mot et compter
by_word = GROUP clean BY word;
counts = FOREACH by_word GENERATE group AS word, COUNT(clean) AS cnt;

-- Trier par fréquence décroissante puis par ordre alphabétique
ordered = ORDER counts BY cnt DESC, word ASC;

-- Enregistrer les résultats
STORE ordered INTO '$OUTPUT' USING PigStorage('\t');
