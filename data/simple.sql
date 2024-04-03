select max(number) from numbers(2222222);
select count(distinct number) from numbers(222222);
select max(number) from numbers(1000000000);
select count(distinct number),
	max(number),
	min(number)
from numbers(1000);
select sum(number) from numbers(1000000000);
