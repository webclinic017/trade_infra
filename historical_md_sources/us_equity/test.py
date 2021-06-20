from datetime import date
from datetime import datetime
my_date = date.today()

print (my_date)
my_time = datetime.min.time()
my_datetime = datetime.combine(my_date, my_time)
print(my_datetime.isoformat())