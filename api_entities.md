Для построения витрины достаточно получить данные с двух api: __/couriers__, __/deliveries__.

Из данных api __/couriers__ получаем идентификатор и имя курьера:
- ___id__; 
- __name__.

Из данных api __/deliveries__ выберем информацию:
- __order_id__ идентификатор заказа;
- __courier_id__ идентификатор курьера;
- __rate__ райтинг доставки;
- __tip_sum__ сумма чаевых.

Остальные данные получены из системы продаж, загружаются в хранилище и распределяются по слоям.


