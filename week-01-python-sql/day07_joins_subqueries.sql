-- Tabla de departamentos
CREATE TABLE departments (
    id      SERIAL PRIMARY KEY,
    name    VARCHAR(100)
);

-- Tabla de empleados
CREATE TABLE employees (
    id            SERIAL PRIMARY KEY,
    name          VARCHAR(100),
    salary        NUMERIC(10,2),
    department_id INTEGER REFERENCES departments(id)
);

-- Inserts
INSERT INTO departments (name) VALUES
    ('Engineering'), ('Marketing'), ('Sales'), ('HR');

INSERT INTO employees (name, salary, department_id) VALUES
    ('Alice',   90000, 1),
    ('Bob',     85000, 1),
    ('Carol',   70000, 2),
    ('David',   65000, 2),
    ('Eve',     80000, 3),
    ('Frank',   60000, NULL),   -- sin departamento
    ('Grace',   95000, 1);



--Ejercicio 7.1 — INNER JOIN
-- Muestra nombre del empleado y nombre de su departamento
-- Solo empleados que TIENEN departamento asignado

select employees.name as employee_name,employees.department_id,
departments.name as assign_department
from departments
INNER JOIN employees
ON departments.id = employees.department_id;



--Ejercicio 7.2 — LEFT JOIN

-- Muestra TODOS los empleados con su departamento
-- Incluye empleados sin departamento (NULL)

select employees.name as employee_name,employees.department_id,
departments.name as assign_department
from employees
LEFT JOIN departments
ON departments.id = employees.department_id;


--Ejercicio 7.3 — Agregación con JOIN
-- Por cada departamento muestra:
-- nombre del depto, total empleados, salario promedio
-- Ordenado por salario promedio DESC


WITH stadistics_dept as(
select employees.name as employee_name,employees.department_id,
departments.name as assign_department,employees.salary as employees_salary
from employees
LEFT JOIN departments
ON departments.id = employees.department_id

)

select assign_department,COUNT(employee_name)total_employeed ,AVG(employees_salary) as Average_salary
from stadistics_dept
GROUP BY assign_department
ORDER BY Average_salary DESC


-- Muestra empleados que trabajan en departamentos
-- cuyo nombre contiene 'ing' (Engineering, Marketing)

--- #2 way
WITH stadistics_dept as(
select employees.name as employee_name,employees.department_id,
departments.name as assign_department,employees.salary as employees_salary
from employees
LEFT JOIN departments
ON departments.id = employees.department_id

)

select assign_department,employee_name
from stadistics_dept
where assign_department like ('%ing%')


--Ejercicio 7.5 — Subquery con IN
-- Muestra empleados que trabajan en departamentos
-- cuyo nombre contiene 'ing' (Engineering, Marketing)

select * from departments
where id IN(select department_id from employees) and name like ('%ing%')

;


	