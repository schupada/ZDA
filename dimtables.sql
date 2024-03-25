-- lab 4

INSERT INTO customer_dim (customer_id, first_name, last_name, email, address_id, create_date, last_update)
SELECT customer_id, first_name, last_name, email, address_id, create_date, last_update
FROM customer;

INSERT INTO date_dim (date, year, quarter, month, day, day_of_week, day_name, is_weekend)
SELECT DISTINCT
    DATE_TRUNC('day', rental_date) AS date,
    EXTRACT(YEAR FROM rental_date) AS year,
    EXTRACT(QUARTER FROM rental_date) AS quarter,
    EXTRACT(MONTH FROM rental_date) AS month,
    EXTRACT(DAY FROM rental_date) AS day,
    (EXTRACT(DOW FROM rental_date) + 6) % 7 AS day_of_week,
    TO_CHAR(rental_date, 'Day') AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM rental_date) IN (6,7) THEN true ELSE false END AS is_weekend
FROM rental;

INSERT INTO rental_fact (customer_id, store_id, date_id, rental_count, rental_revenue, late_fee_revenue)
SELECT
    rental.customer_id,
    store.store_id,
    date_dim.date_id,
    COUNT(rental.rental_id) AS rental_count,
    SUM(payment.amount)     AS rental_revenue,
    SUM(CASE
       WHEN rental.return_date > rental.rental_date + INTERVAL '1 day' * film.rental_duration THEN
           EXTRACT(day FROM
               (rental.return_date - rental.rental_date - INTERVAL '1 day' * film.rental_duration)) *
               film.rental_rate
       ELSE 0 END) AS late_fee_revenue
FROM dvdrental.public.rental
         LEFT JOIN dvdrental.public.payment
                   ON rental.rental_id = payment.rental_id
         LEFT JOIN dvdrental.public.inventory
                   ON rental.inventory_id = inventory.inventory_id
         LEFT JOIN dvdrental.public.film
                   ON inventory.film_id = film.film_id
         LEFT JOIN dvdrental.public.store
                   ON inventory.store_id = store.store_id
         LEFT JOIN date_dim
                   ON DATE(rental.rental_date) = date_dim.date
GROUP BY
    rental.customer_id,
         store.store_id,
         date_dim.date_id;

ALTER TABLE rental_fact ADD interval INT;

-- lab 5 - task 1

CREATE TABLE top_customers (
    customer_id int primary key,
    name varchar(255),
    sum_of_revenue int,
    city varchar(255),
    country varchar(255),
    email varchar(50)
);

INSERT INTO top_customers(customer_id, name, sum_of_revenue, city, country, email)
SELECT
    rental_fact.customer_id,
    customer.first_name || ' ' || customer.last_name AS name,
    SUM(rental_fact.rental_revenue) AS sum_of_revenue,
    city.city,
    country.country,
    customer.email
FROM
    rental_fact
    LEFT JOIN customer on rental_fact.customer_id = customer.customer_id
    LEFT JOIN address on customer.customer_id = address.address_id
    LEFT JOIN city on address.city_id = city.city_id
    LEFT JOIN country on city.country_id = country.country_id
GROUP BY
    rental_fact.customer_id, name, city.city, country.country, customer.email
ORDER BY
    sum_of_revenue DESC
LIMIT 20;

ALTER TABLE rental_fact ADD COLUMN rental_id SERIAL primary key;

-- divide revenue values into 10 buckets of roughly same size

WITH ranked_rentals AS (
    SELECT rental_id, NTILE(10) OVER (ORDER BY rental_revenue) AS ntile_value
    FROM rental_fact
    WHERE rental_revenue IS NOT NULL
)
UPDATE rental_fact
SET interval = ranked_rentals.ntile_value
FROM ranked_rentals
WHERE rental_fact.rental_id = ranked_rentals.rental_id;

-- task 4
ALTER TABLE rental_fact ADD COLUMN name varchar(255);
WITH full_name AS (
    SELECT customer.customer_id, customer.first_name || ' ' || customer.last_name AS name
    FROM customer
)
UPDATE rental_fact
SET name = full_name.name
FROM full_name
WHERE rental_fact.customer_id = full_name.customer_id;

ALTER TABLE rental_fact ADD COLUMN actor_name varchar(255);
/*WITH full_actor_name AS (
    SELECT actor.actor_id, actor.first_name || ' ' || actor.last_name AS name
    FROM actor
)
UPDATE rental_fact
SET actor_name = full_actor_name.name
FROM full_actor_name
LEFT JOIN film_actor ON full_actor_name.actor_id = film_actor.actor_id
LEFT JOIN film on film_actor.film_id = film.film_id
LEFT JOIN inventory on film.film_id = inventory.film_id
LEFT JOIN rental_fact on inventory.inventory_id = rental_fact.rental_id
WHERE film_actor.actor_id = full_actor_name.actor_id;*/