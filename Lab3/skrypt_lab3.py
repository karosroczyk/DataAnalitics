# -*- coding: utf-8 -*-
"""
Created on Sun Oct 27 16:35:05 2019

@author: Lenovo
"""
import psycopg2 as pg 
import pandas.io.sql as psql 
import pandas as pd

connection = pg.connect(host='localhost', port=5432, dbname='postgres', user='postgres', password='*******')
# 1) Wyświetl listę imion i nazwisk menedżerów mieszkających w tym samym kraju i pracujących w tym sam sklep.
df = pd.read_sql_query("SELECT GROUP_CONCAT(first_name), GROUP_CONCAT(last_name), country, store.store_id\
                       FROM staff\
                       INNER JOIN address a on a.address_id = staff.address_id\
                       INNER JOIN city on city.city_id = a.city_id\
                       INNER JOIN country on country.country_id = city.country_id\
                       INNER JOIN store on store.manager_staff_id = staff.staff_id\
                       GROUP BY country, store.store_id",con=connection) 
print(df)

# 2) Znajdź listę wszystkich filmów o tej samej długości.
df = pd.read_sql_query("SELECT GROUP_CONCAT(title), length \
                       FROM film\
                       GROUP BY length\
                       ORDER BY length",con=connection) 
print(df)

# 3) Znajdź wszystkich klientów mieszkających w tym samym mieście.
df = pd.read_sql_query("SELECT GROUP_CONCAT(last_name) as Customer, city\
                       FROM customer\
                       INNER JOIN address on address.address_id = customer.address_id\
                       INNER JOIN city on address.city_id = city.city_id\
                       GROUP BY city\
                       ORDER BY city;",con=connection) 
print(df)

#4) Oblicz średni koszt wyporzycenia wszystkich filmów.
df = pd.read_sql_query("SELECT AVG(rental_rate)\
                       FROM film",con=connection) 
print(df)

# 5) Oblicz i wyświetl liczbę filmów we wszystkich kategoriach.
df = pd.read_sql_query("SELECT count(title), name\
                       FROM film\
                       INNER JOIN film_category fc on film.film_id = fc.film_id\
                       INNER JOIN category c on fc.category_id = c.category_id\
                       GROUP BY name\
                       ORDER BY name",con=connection) 
print(df)

#6) Wyświetl liczbę wszystkich klientów pogrupowanych według kraju.
df = pd.read_sql_query("SELECT count(customer_id), country\
                       from customer c\
                       INNER JOIN address a on c.address_id = a.address_id\
                       INNER JOIN city on city.city_id = a.city_id\
                       INNER JOIN  country on country.country_id = city.country_id\
                       GROUP BY country\
                       ORDER BY country",con=connection) 
print(df)

# 7) Wyświetl informacje o sklepie, który ma więcej niż 100 klientów i mniej niż 300 klientów.
df = pd.read_sql_query("SELECT s.store_id, s.manager_staff_id, s.address_id, s.last_update,\
                       count(customer_id) number\
                       FROM store s\
                       INNER JOIN customer c on c.store_id = s.store_id\
                       GROUP BY s.store_id\
                       HAVING count(customer_id) between 100 and 300",con=connection) 
print(df)

# 8) Wybierz wszystkich klientów, którzy oglądali filmy ponad 200 godzin.
df = pd.read_sql_query("SELECT count(film.film_id) ile_filmow, sum(film.length)/60 ile_godzin, c.customer_id\
                       FROM film\
                       INNER JOIN inventory i on i.film_id = film.film_id\
                       INNER JOIN rental r on i.inventory_id = r.inventory_id\
                       INNER JOIN customer c on c.customer_id = r.customer_id\
                       GROUP BY c.customer_id\
                       ORDER BY customer_id",con=connection) 
print(df)

# 8 a) Ile godzin maja wypożyczony jakiś jeden film
df = pd.read_sql_query("SELECT customer_id, sum (DATE_PART('day', return_date::timestamp - rental_date::timestamp) * 24 + \
                                                 DATE_PART('hour', return_date::timestamp - rental_date::timestamp)) ile_godzin \
                	   FROM rental\
                       GROUP BY customer_id\
	                   HAVING SUM(DATE_PART('day', return_date::timestamp - rental_date::timestamp) * 24 + \
                                   DATE_PART('hour', return_date::timestamp - rental_date::timestamp)) > 200\
	                   ORDER BY customer_id",con=connection) 
print(df)

# 9) Oblicz średnią wartość wypożyczenia filmu. To samo co 4 ?
df = pd.read_sql_query("SELECT AVG(amount) srednia \
                       FROM payment",con=connection) 
print(df)

# 10) Oblicz średnią wartość długości filmu we wszystkich kategoriach.
df = pd.read_sql_query("SELECT AVG(length), c.name\
                       FROM film\
                       INNER JOIN film_category fc on film.film_id = fc.film_id\
                       INNER JOIN category c on c.category_id = fc.category_id\
                       GROUP BY name\
                       ORDER BY name",con=connection) 
print(df)

# 11)  Znajdź najdłuższe tytuły filmowe we wszystkich kategoriach.
df = pd.read_sql_query("SELECT c.name, max(length(film.title))\
                       FROM film\
                       INNER JOIN film_category fc on film.film_id = fc.film_id\
                       INNER JOIN category c on c.category_id = fc.category_id\
                       GROUP BY name\
                       ORDER BY name",con=connection) 
print(df)

# 12) Znajdź najdłuższy film we wszystkich kategoriach. Porównaj wynik z pkt 10.
df = pd.read_sql_query("SELECT MAX(length), c.name\
                       FROM film\
                       INNER JOIN film_category fc on film.film_id = fc.film_id\
                       INNER JOIN category c on c.category_id = fc.category_id\
                       GROUP BY name\
                       ORDER BY name",con=connection) 
print(df)
