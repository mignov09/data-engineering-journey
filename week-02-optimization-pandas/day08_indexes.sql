CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER,
    product     VARCHAR(100),
    amount      NUMERIC(10,2),
    status      VARCHAR(50),
    created_at  DATE
);

-- Inserts de prueba
INSERT INTO orders (customer_id, product, amount, status, created_at)
SELECT
    (random() * 1000)::INTEGER,
    (ARRAY['Laptop','Phone','Tablet','Monitor','Keyboard'])[floor(random()*5+1)],
    (random() * 2000)::NUMERIC(10,2),
    (ARRAY['pending','completed','cancelled'])[floor(random()*3+1)],
    CURRENT_DATE - (random() * 365)::INTEGER
FROM generate_series(1, 10000);



--Ejercicio 8.1 — EXPLAIN ANALYZE antes del índice
-- Ejecuta esto y observa el "cost" y "rows"
EXPLAIN ANALYZE
SELECT * FROM orders WHERE customer_id = 5;


---Ejercicio 8.2 — Crear índice simple
-- Crea un índice en customer_id
-- Luego repite el EXPLAIN ANALYZE del 8.1
-- ¿Cambió el costo?

CREATE INDEX idx_customer_id ON orders(customer_id)



---Ejercicio 8.3 — Índice en columna de fecha
-- Crea un índice en created_at
CREATE INDEX idx_created_at ON orders(created_at)
-- Luego ejecuta y analiza:
EXPLAIN ANALYZE
SELECT * FROM orders
WHERE created_at BETWEEN '2024-01-01' AND '2024-06-30';


--Ejercicio 8.4 — Índice compuesto

-- Crea un índice sobre (status, customer_id) juntos
CREATE INDEX idx_compound ON orders(status, customer_id);


-- ¿Cuándo conviene un índice compuesto?
Orden de columnas importa
-- cuando esas columnas se filtran juntas se recomienda



-- Consulta los índices existentes de la tabla orders
-- Usa la vista pg_indexes
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'orders'
ORDER BY indexname;