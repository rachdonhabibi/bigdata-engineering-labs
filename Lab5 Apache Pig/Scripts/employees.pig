%default EMPLOYEES '/user/root/input/employees';
%default DEPARTMENTS '/user/root/input/departments';
%default OUTPUT '/user/root/pigout/employees';
%default HIGH_SAL 3000;

-- Chargement des deux jeux (employés: 7 champs / départements: 2 champs)
emp_src = LOAD '$EMPLOYEES' USING PigStorage('\t') AS (
    emp_id:int,
    nom_complet:chararray,
    sexe:chararray,
    salaire:int,
    depno:int,
    ville:chararray,
    pays:chararray
);

dept_src = LOAD '$DEPARTMENTS' USING PigStorage('\t') AS (
    depno:int,
    dep_nom:chararray
);

-- Pré-projections réutilisables
emp_basic = FOREACH emp_src GENERATE emp_id, depno, salaire, ville, sexe, nom_complet;
dept_only = FOREACH dept_src GENERATE depno, dep_nom;

-- Agrégations par département (regroupement unique)
grp_dep = GROUP emp_basic BY depno;
dep_avg = FOREACH grp_dep GENERATE group AS depno, AVG(emp_basic.salaire) AS sal_moyen;
STORE dep_avg INTO '$OUTPUT/avg_sal' USING PigStorage('\t');

dep_count = FOREACH grp_dep GENERATE group AS depno, COUNT(emp_basic) AS nb_emp;
STORE dep_count INTO '$OUTPUT/count_dep' USING PigStorage('\t');

-- Filtrage des salaires élevés
sal_haut = FILTER emp_basic BY salaire > $HIGH_SAL;
STORE sal_haut INTO '$OUTPUT/high_sal' USING PigStorage('\t');

-- Extraction des top salaires par département
dep_max = FOREACH grp_dep GENERATE group AS depno, MAX(emp_basic.salaire) AS max_dep_sal;
jointure_top = JOIN emp_basic BY depno, dep_max BY depno;
selection_top = FILTER jointure_top BY emp_basic::salaire == dep_max::max_dep_sal;
projection_top = FOREACH selection_top GENERATE emp_basic::depno AS depno,
                                                                                        emp_basic::emp_id AS id,
                                                                                        emp_basic::nom_complet AS nom,
                                                                                        emp_basic::salaire AS salaire;
STORE projection_top INTO '$OUTPUT/top_dep' USING PigStorage('\t');

-- Départements sans aucun employé (LEFT JOIN + IS NULL)
dep_present = DISTINCT (FOREACH emp_basic GENERATE depno);
dept_left = JOIN dept_only BY depno LEFT, dep_present BY depno;
dept_vides = FILTER dept_left BY dep_present::depno IS NULL;
dept_vides_out = FOREACH dept_vides GENERATE dept_only::depno AS depno, dept_only::dep_nom AS dep_nom;
STORE dept_vides_out INTO '$OUTPUT/dep_empty' USING PigStorage('\t');

-- Total global d'employés
grp_all = GROUP emp_basic ALL;
nb_total = FOREACH grp_all GENERATE COUNT(emp_basic) AS total_employees;
STORE nb_total INTO '$OUTPUT/total_emp' USING PigStorage('\t');

-- Employés dont la ville (normalisée) = PARIS
paris_emps = FILTER emp_basic BY UPPER(ville) == 'PARIS';
STORE paris_emps INTO '$OUTPUT/paris_emp' USING PigStorage('\t');

-- Somme des salaires par ville
grp_ville = GROUP emp_basic BY ville;
sal_ville = FOREACH grp_ville GENERATE group AS ville, SUM(emp_basic.salaire) AS sal_total;
STORE sal_ville INTO '$OUTPUT/sum_city' USING PigStorage('\t');

-- Départements contenant au moins une employée (F)
emp_f = FILTER emp_basic BY UPPER(sexe) == 'F';
dep_f = DISTINCT (FOREACH emp_f GENERATE depno);
dep_f_named = JOIN dep_f BY depno, dept_only BY depno;
deps_emp_f = FOREACH dep_f_named GENERATE dept_only::depno AS depno, dept_only::dep_nom AS dep_nom;
STORE deps_emp_f INTO '$OUTPUT/employes_femmes' USING PigStorage('\t');
