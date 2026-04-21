CREATE TABLE accounts (
    id      SERIAL PRIMARY KEY,
    name    VARCHAR(100),
    balance NUMERIC(10,2)
);

INSERT INTO accounts (name, balance) VALUES
    ('Alice', 5000.00),
    ('Bob',   3000.00),
    ('Carol', 8000.00);


	---Ejercicio 10.1  Transacción básica
--
-- Transfiere $500 de Alice a Bob en una sola transacción
-- Pasos:
-- 1. BEGIN
BEGIN;
-- 2. Descuenta $500 a Alice
    UPDATE accounts SET balance = balance - 500 WHERE name = 'Alice';
	-- 3. Agrega $500 a Bob
    UPDATE accounts SET balance = balance + 500 WHERE name = 'Bob';
	-- 5. COMMIT
COMMIT;

-- 4. Verifica los saldos con SELECT
select *  from accounts




--Ejercicio 10.2 — ROLLBACK manual
--sql
-- Inicia una transacción
BEGIN;

-- Intenta transferir $10,000 de Bob (solo tiene $3,000)
 UPDATE accounts SET balance = balance -7000 WHERE name = 'Bob';

ROLLBACK;



-- Haz ROLLBACK antes del COMMIT
COMMIT;

-- Verifica que los saldos no cambiaron
select *  from accounts



--Ejercicio 10.3 — SAVEPOINT

-- BEGIN
BEGIN;
-- Descuenta $200 a Carol
UPDATE accounts SET balance = balance -200 WHERE name = 'Carol';

-- SAVEPOINT antes_de_bob

SAVEPOINT antes_de_bob;

-- Agrega $200 a Bob

 UPDATE accounts SET balance = balance + 200 WHERE name = 'Bob';
 
-- ROLLBACK TO antes_de_bob   ← deshace solo la parte de Bob

ROLLBACK TO antes_de_bob;


-- COMMIT                      ← confirma solo el descuento a Carol
COMMIT;

-- Verifica saldos
SELECT * FROM accounts;



--Ejercicio 10.4 — Transacción con validación
-- Intenta transferir $500 de Alice a Bob
DO $$
DECLARE
  saldo_alice NUMERIC;
BEGIN
  -- 1. Verificar saldo actual de Alice
  SELECT balance INTO saldo_alice
  FROM accounts
  WHERE name = 'Alice';

  -- 2. Si tiene suficiente saldo → hacer la transferencia
  IF saldo_alice > 500 THEN
    UPDATE accounts SET balance = balance - 500 WHERE name = 'Alice';
    UPDATE accounts SET balance = balance + 500 WHERE name = 'Bob';
    RAISE NOTICE 'Transferencia exitosa. Saldo Alice: %', saldo_alice - 500;

  -- 3. Si no tiene suficiente → cancelar
  ELSE
    RAISE NOTICE 'Saldo insuficiente. Alice solo tiene: %', saldo_alice;
    ROLLBACK;
  END IF;

END;
$$;



