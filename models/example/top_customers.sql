With days_difference as (
select inventory_id, extract(day from return_date - rental_date) as days_diff
from {{ source ('cross_border_analytics', 'rental') }} rental )
select case when days_diff > film.rental_duration then 'returned_late' 
	    when days_diff < film.rental_duration then 'returned_early' 
	    when days_diff = film.rental_duration then 'returned_on_time' end as rental_report
from days_difference   
join {{ source ('cross_border_analytics', 'inventory') }} inventory 
on inventory.inventory_id = days_difference.inventory_id
join {{ source ('cross_border_analytics', 'film') }} film 
on inventory.film_id = film.film_id
