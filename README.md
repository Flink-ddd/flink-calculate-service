flink-calculate-service

this project is base on flink 1.11.1 technology, combina redis, kafka technology etc. location statistics-center pre-service flink-culculate-service

1: consume each business Microservice’s data topic, such as place on order topic, user buy record topic, business enter topic etc as well as some diffcult
data topic.

2: create public aggregation statistics method or core flow, according to different dimensionality realize statistics calculate, for example, according to
gender,band, commodity type, area etc dimensionality to statisstics order total sales volume and total Amount.

3: create calculate main service, read kafka consumer infomation, Puts operators of different dimensions in the execution job of the main service, The bottom 
level calls the common aggregate statistics method or the core process to calculate

4: Store the calculated data in Redis according to the key in different dimensions and data superposition calculation. Or according to the data table 
in different dimensions. Stored in MySQL. 

5: The statistics center will encapsulate the relevant statistical interface, query the relevant Redis or MySQL, and provide front-end display.
