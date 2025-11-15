%default EMPLOYEES '/user/root/input/employees';
%default DEPARTMENTS '/user/root/input/departments';
%default OUTPUT '/user/root/pigout/employees';
%default HIGH_SAL 3000;

-- Schémas : employés (7 colonnes) et départements (2 colonnes)
employees = LOAD '$EMPLOYEES' USING PigStorage('\t') AS (
    id:int,
    full_name:chararray,
    gender:chararray,
    salary:int,
    depno:int,
    city:chararray,
    country:chararray
);

departments = LOAD '$DEPARTMENTS' USING PigStorage('\t') AS (
    depno:int,
    dep_name:chararray
);

-- Salaire moyen par département
emp_by_dep = GROUP employees BY depno;
avg_sal = FOREACH emp_by_dep GENERATE group AS depno, AVG(employees.salary) AS avg_salary;
STORE avg_sal INTO '$OUTPUT/avg_sal' USING PigStorage('\t');

-- Nombre d'employés par département
count_dep = FOREACH emp_by_dep GENERATE group AS depno, COUNT(employees) AS emp_count;
STORE count_dep INTO '$OUTPUT/count_dep' USING PigStorage('\t');

-- Employés à salaire élevé (> $HIGH_SAL)
high_sal = FILTER employees BY salary > $HIGH_SAL;
STORE high_sal INTO '$OUTPUT/high_sal' USING PigStorage('\t');

-- Meilleur salaire par département (employés au salaire max de leur département)
max_sal_by_dep = FOREACH emp_by_dep GENERATE group AS depno, MAX(employees.salary) AS max_salary;
emp_max_join = JOIN employees BY depno, max_sal_by_dep BY depno;
top_dep = FILTER emp_max_join BY employees::salary == max_sal_by_dep::max_salary;
top_dep_proj = FOREACH top_dep GENERATE employees::depno AS depno,
                                   employees::id AS id,
                                   employees::full_name AS full_name,
                                   employees::salary AS salary;
STORE top_dep_proj INTO '$OUTPUT/top_dep' USING PigStorage('\t');

-- Départements sans employés
emp_depnos = FOREACH employees GENERATE depno;
emp_depnos_distinct = DISTINCT emp_depnos;
left_join = JOIN departments BY depno LEFT, emp_depnos_distinct BY depno;
dep_empty = FILTER left_join BY emp_depnos_distinct::depno IS NULL;
dep_empty_proj = FOREACH dep_empty GENERATE departments::depno AS depno, departments::dep_name AS dep_name;
STORE dep_empty_proj INTO '$OUTPUT/dep_empty' USING PigStorage('\t');

-- Nombre total d'employés
total_grp = GROUP employees ALL;
total_emp = FOREACH total_grp GENERATE COUNT(employees) AS total_employees;
STORE total_emp INTO '$OUTPUT/total_emp' USING PigStorage('\t');

-- Employés basés à Paris
paris_emp = FILTER employees BY UPPER(city) == 'PARIS';
STORE paris_emp INTO '$OUTPUT/paris_emp' USING PigStorage('\t');

-- Somme des salaires par ville
by_city = GROUP employees BY city;
sum_city = FOREACH by_city GENERATE group AS city, SUM(employees.salary) AS total_salary;
STORE sum_city INTO '$OUTPUT/sum_city' USING PigStorage('\t');

-- Départements avec au moins une employée (F)
ef = FILTER employees BY UPPER(gender) == 'F';
ef_deps = DISTINCT (FOREACH ef GENERATE depno);
ef_deps_named = JOIN ef_deps BY depno, departments BY depno;
employes_femmes = FOREACH ef_deps_named GENERATE departments::depno AS depno, departments::dep_name AS dep_name;
STORE employes_femmes INTO '$OUTPUT/employes_femmes' USING PigStorage('\t');
