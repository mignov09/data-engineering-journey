WITH employee_totals AS (
    SELECT
        employee_id,
        COUNT(sale_id)AS total_sales,
        SUM(amount)AS total_amount,
        AVG(amount)AS avg_amount,
        MAX(amount)AS best_sale
    FROM sales
    GROUP BY employee_id
)

-- join with employees to get names
SELECT
    rank() over(ORDER BY et.total_amount DESC) as top_employees,
    e.employee_name,
    e.department,
    e.region,
    et.total_sales,
	 et.total_amount,
    ROUND(et.avg_amount, 2) AS avg_amount,
    et.best_sale
FROM employee_totals et
JOIN employees e ON et.employee_id = e.employee_id
WHERE total_amount > 3000
ORDER BY et.total_amount DESC;









