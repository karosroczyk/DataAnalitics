# -*- coding: utf-8 -*-
"""
Created on Sun Oct 27 16:35:05 2019

@author: Lenovo
"""
import psycopg2 as pg 
import pandas.io.sql as psql 
import pandas as pd

connection = pg.connect(host='localhost', port=5432, dbname='postgres', user='postgres', password='*********')
# 1) Ile kategorii filmów mamy w wypożyczalni?
df = pd.read_sql_query("select count(category_id) from category",con=connection) 
print(df)

# 2) Wyświetl listę kategorii w kolejności alfabetycznej.
df = pd.read_sql_query("select name from category order by name",con=connection) 
print(df)

# 3) Znajdź najstarszy i najmłodszy film do wypożyczenia.
df = pd.read_sql_query("select title, release_year from film where release_year = (select min(release_year) from film)",con=connection) 
print(df)

df = pd.read_sql_query("select title, release_year from film where release_year = (select max(release_year) from film)",con=connection) 
print(df)

#4) Ile wypożyczeń odbyło się między 2005-07-01 a 2005-08-01
df = pd.read_sql_query("select count(rental_date) from rental where rental_date between'2005-07-01' and '2005-08-01'",con=connection) 
print(df)

# 5) Ile wypożyczeń odbyło się między 2010-01-01 a 2011-02-01
df = pd.read_sql_query("select count(rental_date) from rental where rental_date between'2010-01-01' and '2011-02-01'",con=connection) 
print(df)

#6) Znajdź największą płatność wypożyczenia
df = pd.read_sql_query("select max(amount) from payment",con=connection) 
print(df)

# 7) Znajdź wszystkich klientów z Polski, Nigerii lub Bangladeszu.
df = pd.read_sql_query("select first_name, last_name from customer where address_id in(select address_id from address where city_id in(select city_id from city where country_id in (select country_id from country where country in('Poland', 'Bangladesh', 'Nigeria'))))",con=connection) 
print(df)

# 8) Gdzie mieszkają członkowie personelu?
df = pd.read_sql_query("select country from country where country_id in(select country_id from city where city_id in(select city_id from address where address_id in(select address_id from staff)))",con=connection) 
print(df)

# 9) Ilu pracowników mieszka w Argentynie lub Hiszpanii?
df = pd.read_sql_query("select count(staff_id) from staff where address_id in(select address_id from address where city_id in(select city_id from city where country_id in (select country_id from country where country in ('Spain', 'Argentina'))))",con=connection) 
print(df)

# 10) Jakie kategorie filmów zostały wypożyczone przez klientów?
df = pd.read_sql_query("select name from category where category_id in(select category_id from film_category where film_id in(select film_id from inventory where inventory_id in(select inventory_id from rental where customer_id in(select customer_id from customer))))",con=connection) 
print(df)

# 11) Znajdź wszystkie kategorie filmów wypożyczonych w Ameryce.
df = pd.read_sql_query("select name from category where category_id in(select category_id from film_category where film_id in(select film_id from inventory where inventory_id in(select inventory_id from rental where customer_id in(select customer_id from customer where address_id in(select address_id from address where city_id in(select city_id from city where country_id in(select country_id from country where country = 'United States')))))))",con=connection) 
print(df)

# 12) Znajdź wszystkie tytuły filmów, w których grał: Olympia Pfeiffer lub Julia Zellweger lub Ellen Presley
df = pd.read_sql_query("select title from film where film_id in(select film_id from film_actor where actor_id in(select actor_id from actor where first_name = 'Olympia' and last_name = 'Pfeiffer' or first_name = 'Julia' and last_name = 'Zellweger' or first_name = 'Ellen' and last_name = 'Presley' ))",con=connection) 
print(df)
